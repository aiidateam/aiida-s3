"""Helpers for namespaced shared-object keys and node repository metadata manipulation."""

from __future__ import annotations

import typing as t

from .repository.multi import make_shared_key, split_shared_key

__all__ = (
    'add_shared_repository',
    'attach_shared_object',
    'get_shared_repositories',
    'iter_repository_keys',
    'remap_repository_metadata',
    'remove_shared_repository',
)


def get_shared_repositories() -> list[dict[str, t.Any]]:
    """Return the shared repositories registered in the current profile's storage configuration.

    :return: list of the shared repository configuration dictionaries, each containing the keys ``name``,
        ``endpoint_url``, ``access_key_id``, ``secret_access_key`` and ``bucket_name``.
    """
    from aiida.manage.configuration import get_profile

    profile = get_profile()
    assert profile is not None, 'No profile has been loaded.'
    entries: list[dict[str, t.Any]] = profile.storage_config.get('shared_repositories', [])
    return entries


def add_shared_repository(
    name: str, endpoint_url: str, access_key_id: str, secret_access_key: str, bucket_name: str
) -> None:
    """Register a read-only shared repository in the current profile's storage configuration and persist it.

    The bucket is probed for existence before the configuration is written, such that the command fails fast if the
    bucket is not reachable.

    :param name: the local name of the shared repository. Must not be empty or contain slashes, since it is used to
        build the namespaced object keys.
    :param endpoint_url: the endpoint URL of the server hosting the shared bucket.
    :param access_key_id: the access key ID to use to authenticate with S3.
    :param secret_access_key: the secret access key to use to authenticate with S3.
    :param bucket_name: the name of the shared bucket.
    :raises ValueError: if the name is invalid or already registered.
    :raises FileNotFoundError: if the bucket does not exist or is not reachable.
    """
    from aiida.manage.configuration import get_config, get_profile

    from .repository.s3 import S3RepositoryBackend

    if not name or '/' in name:
        raise ValueError('The shared repository name must be non-empty and must not contain `/`.')

    profile = get_profile()
    assert profile is not None, 'No profile has been loaded.'

    entries = profile.storage_config.setdefault('shared_repositories', [])
    if any(entry['name'] == name for entry in entries):
        raise ValueError(f'A shared repository named `{name}` is already registered.')

    if not S3RepositoryBackend(
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        bucket_name=bucket_name,
    ).is_initialised:
        raise FileNotFoundError(f'Bucket `{bucket_name}` does not exist or is not reachable.')

    entries.append(
        {
            'name': name,
            'endpoint_url': endpoint_url,
            'access_key_id': access_key_id,
            'secret_access_key': secret_access_key,
            'bucket_name': bucket_name,
        }
    )

    config = get_config()
    config.update_profile(profile)
    config.store()


def remove_shared_repository(name: str) -> None:
    """Remove the shared repository registered under ``name`` from the current profile's storage configuration.

    :param name: the local name of the shared repository.
    :raises ValueError: if no shared repository is registered under ``name``.
    """
    from aiida.manage.configuration import get_config, get_profile

    profile = get_profile()
    assert profile is not None, 'No profile has been loaded.'

    entries = profile.storage_config.get('shared_repositories', [])
    if not any(entry['name'] == name for entry in entries):
        raise ValueError(f'No shared repository named `{name}` is registered.')

    profile.storage_config['shared_repositories'] = [entry for entry in entries if entry['name'] != name]

    config = get_config()
    config.update_profile(profile)
    config.store()


def remap_repository_metadata(metadata: dict[str, t.Any], shared_name: str) -> dict[str, t.Any]:
    """Return a deep copy of node repository metadata with every object key namespaced for a shared repository.

    ``<uuid>`` is mapped to ``shared/<shared_name>/<uuid>``. Only the ``'k'`` fields are rewritten; the directory
    structure is preserved as-is. Keys that are already namespaced are re-namespaced under ``shared_name``.

    :param metadata: the serialized repository metadata of a node, in the aiida-core ``{'o': ...}``/``{'k': ...}``
        format.
    :param shared_name: the name under which the shared repository is registered in the importing profile.
    :return: the remapped metadata.
    """
    if 'k' in metadata:
        parts = split_shared_key(metadata['k'])
        object_key = parts[1] if parts else metadata['k']
        return {'k': make_shared_key(shared_name, object_key)}
    if not metadata:
        return {}
    return {'o': {name: remap_repository_metadata(child, shared_name) for name, child in metadata.get('o', {}).items()}}


def iter_repository_keys(metadata: dict[str, t.Any], prefix: str = '') -> t.Iterator[tuple[str, str]]:
    """Yield the ``(path, key)`` tuple for every file entry in node repository metadata.

    :param metadata: the serialized repository metadata of a node, in the aiida-core ``{'o': ...}``/``{'k': ...}``
        format.
    :param prefix: the path prefix accumulated during recursion.
    :return: iterator of tuples of the file path within the node repository and the fully qualified object key.
    """
    if 'k' in metadata:
        yield prefix, metadata['k']
    for name, child in metadata.get('o', {}).items():
        yield from iter_repository_keys(child, f'{prefix}/{name}' if prefix else name)


def attach_shared_object(node: t.Any, path: str, shared_name: str, object_key: str, verify: bool = True) -> None:
    """Attach an existing object of a shared repository to ``node`` at ``path``, without moving any bytes.

    The node must be stored. The object's existence in the shared bucket is verified, unless ``verify`` is set to
    ``False``, and the node's repository metadata is updated such that ``path`` resolves to
    ``shared/<shared_name>/<object_key>``.

    :param node: the stored node to attach the object to.
    :param path: the path within the node's repository at which to reference the object.
    :param shared_name: the name under which the shared repository is registered in the current profile.
    :param object_key: the backend-local (original) key of the object in the shared bucket.
    :param verify: whether to verify the object exists in the shared repository before attaching it.
    :raises FileNotFoundError: if the object does not exist in the shared repository and ``verify`` is True.
    """
    from aiida.manage import get_manager

    repository = get_manager().get_profile_storage().get_repository()
    assert hasattr(repository, 'verify_shared'), 'Storage backend is not a multi-repository backend.'

    key = make_shared_key(shared_name, object_key)

    if verify and not repository.has_objects([key])[0]:
        raise FileNotFoundError(f'Object `{object_key}` not found in shared repository `{shared_name}`.')

    metadata = dict(node.backend_entity.repository_metadata or {})
    entry: dict[str, t.Any] = metadata.setdefault('o', {})
    for part in path.strip('/').split('/')[:-1]:
        next_entry = entry.setdefault(part, {})
        if 'k' in next_entry:
            raise ValueError(f'Path segment `{part}` of `{path}` already exists as a file.')
        entry = next_entry.setdefault('o', {})
    leaf = path.strip('/').split('/')[-1]
    entry[leaf] = {'k': key}
    node.backend_entity.repository_metadata = metadata
    # The node's repository interface caches the file hierarchy built from the metadata at store time. Reset the
    # cached repository instance, such that the attached object is visible on the next access.
    node.base.repository._repository_instance = None
