"""Export and import of shared-repository archives.

A shared-repository archive is a regular aiida-core archive (SQLite-zip format), except that:

* it contains no object bytes: all ``repo/`` members are stripped;
* every node's ``repository_metadata`` object keys are namespaced as ``shared/<name>/<object-key>``, where ``<name>``
  is the name under which importing profiles register the *source* bucket as a read-only shared repository;
* its ``metadata.json`` carries a manifest (``shared_repository`` entry) with the coordinates of the source bucket.

Importing such an archive materializes the entities in the target profile's database and makes the nodes reference
the objects in the shared bucket by key. No S3 upload or download of object bytes ever occurs.
"""

from __future__ import annotations

import contextlib
import json
import pathlib
import sqlite3
import tempfile
import typing as t
import zipfile

from .repository.multi import split_shared_key
from .shared import add_shared_repository, iter_repository_keys, remap_repository_metadata

__all__ = ('SHARED_ARCHIVE_FORMAT', 'export_shared', 'import_shared')

SHARED_ARCHIVE_FORMAT = 'aiida-s3-shared/1'
METADATA_NAME = 'metadata.json'
DATABASE_NAME = 'db.sqlite3'
REPO_FOLDER = 'repo'


def export_shared(name: str, entities: t.Iterable[t.Any], output: pathlib.Path, verify: bool = True) -> None:
    """Export ``entities`` as a shared-repository archive, containing no object bytes.

    The pipeline is:

    1. optionally verify the existence of every referenced object in the primary bucket (metadata requests only);
    2. create a standard, byte-complete aiida-core archive as a temporary artifact;
    3. remap every node ``repository_metadata`` object key to ``shared/<name>/<object-key>``, inject the
       shared-repository manifest into ``metadata.json`` and strip all object bytes from the archive;
    4. write the result to ``output``.

    :param name: the name under which importing profiles will register the source bucket as a shared repository.
    :param entities: the entities to export: nodes, groups and/or computers.
    :param output: the path of the shared-repository archive to create.
    :param verify: whether to verify that every referenced object exists in the primary bucket before exporting.
    :raises ValueError: if any of the nodes references an object in a shared repository itself, since the bytes of
        such objects would not be reachable through the exported bucket.
    :raises FileNotFoundError: if ``verify`` is True and a referenced object is missing from the primary bucket.
    """
    from aiida import orm
    from aiida.manage import get_manager
    from aiida.tools.archive.create import create_archive

    backend = get_manager().get_profile_storage()
    repository = backend.get_repository()

    entities = list(entities)
    keys: list[str] = []
    for node in [entity for entity in entities if isinstance(entity, orm.Node)]:
        keys.extend(key for _, key in iter_repository_keys(node.backend_entity.repository_metadata or {}))

    if any(split_shared_key(key) is not None for key in keys):
        raise ValueError(
            'Cannot export nodes that reference objects in shared repositories themselves: the bytes would not be '
            'reachable through the exported bucket. Export the original profile instead.'
        )

    if verify and keys:
        missing = [key for key, exists in zip(keys, repository.has_objects(keys)) if not exists]
        if missing:
            raise FileNotFoundError(f'Referenced objects missing from the primary bucket: {missing}')

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_archive = pathlib.Path(tmpdir) / 'archive.aiida'
        create_archive(
            entities=entities,
            filename=tmp_archive,
            archive_format=repository.archive_format,
            backend=backend,
        )

        with zipfile.ZipFile(tmp_archive, 'r') as archive:
            metadata = json.loads(archive.read(METADATA_NAME))
            db_path = pathlib.Path(tmpdir) / DATABASE_NAME
            db_path.write_bytes(archive.read(DATABASE_NAME))

        _remap_archive_node_keys(db_path, name)

        metadata['shared_repository'] = {
            'format': SHARED_ARCHIVE_FORMAT,
            'name': name,
            'endpoint_url': backend.profile.storage_config['endpoint_url'],
            'bucket_name': backend.profile.storage_config['bucket_name'],
            'key_format': repository.key_format,
        }

        with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as out:
            out.writestr(METADATA_NAME, json.dumps(metadata))
            out.write(db_path, DATABASE_NAME)


def import_shared(archive_path: pathlib.Path, credentials: dict[str, str] | None = None) -> None:
    """Import a shared-repository archive into the current profile, transferring no object bytes.

    The pipeline is:

    1. read the manifest and, if ``credentials`` is provided, register the source bucket as a shared repository;
    2. verify that the shared repository is registered and reachable in this profile;
    3. verify that every referenced shared object exists in the shared bucket (failing before anything is written);
    4. rehydrate a standard archive with placeholder bytes and import it with the standard aiida-core machinery. The
       placeholder bytes are discarded by :class:`aiida_s3.repository.multi.MultiS3RepositoryBackend`: the objects are
       adopted by reference after an existence check;
    5. post-verify that every referenced key resolves in the shared bucket.

    :param archive_path: the path of the shared-repository archive to import.
    :param credentials: optional dictionary with ``access_key_id`` and ``secret_access_key`` used to register the
        shared repository if it is not registered yet. Credentials are never stored in the archive.
    :raises ValueError: if the archive is not a shared-repository archive, if the shared repository is not
        registered, or if the archive contains non-namespaced object keys.
    :raises FileNotFoundError: if referenced objects are missing from the shared bucket.
    """
    from aiida.manage import get_manager
    from aiida.tools.archive.imports import import_archive

    with zipfile.ZipFile(archive_path, 'r') as archive:
        names = archive.namelist()
        if METADATA_NAME not in names or DATABASE_NAME not in names:
            raise ValueError(f'`{archive_path}` is not an aiida archive.')
        metadata = json.loads(archive.read(METADATA_NAME))

    manifest = metadata.get('shared_repository', {})
    if manifest.get('format') != SHARED_ARCHIVE_FORMAT:
        raise ValueError(f'`{archive_path}` is not a shared-repository archive; use `verdi archive import` instead.')

    name = manifest['name']

    if credentials is not None:
        add_shared_repository(
            name=name,
            endpoint_url=manifest['endpoint_url'],
            access_key_id=credentials['access_key_id'],
            secret_access_key=credentials['secret_access_key'],
            bucket_name=manifest['bucket_name'],
        )

    backend = get_manager().get_profile_storage()
    repository = _ensure_multi_repository(backend)

    if not repository.verify_shared().get(name, False):
        raise ValueError(
            f'Shared repository `{name}` is not registered or not reachable in this profile. '
            'Register it with `aiida-s3 shared add` or pass `--register`.'
        )

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(archive_path, 'r') as archive:
            db_path = pathlib.Path(tmpdir) / DATABASE_NAME
            db_path.write_bytes(archive.read(DATABASE_NAME))

        keys = _collect_archive_node_keys(db_path)

        if keys:
            missing = [key for key, exists in zip(keys, repository.has_objects(keys)) if not exists]
            if missing:
                raise FileNotFoundError(f'Referenced objects missing from the shared repository: {missing}')

        tmp_archive = pathlib.Path(tmpdir) / 'rehydrated.aiida'
        with zipfile.ZipFile(tmp_archive, 'w', zipfile.ZIP_DEFLATED) as out:
            out.writestr(METADATA_NAME, json.dumps(metadata))
            out.write(db_path, DATABASE_NAME)
            for key in keys:
                # Placeholder bytes: never stored anywhere persistent and never uploaded. They only serve to satisfy
                # the archive validation of aiida-core's import machinery.
                out.writestr(f'{REPO_FOLDER}/{key}', b'')

        with _preserve_archive_keys():
            import_archive(tmp_archive, archive_format=repository.archive_format, backend=backend)

    if keys and not all(repository.has_objects(keys)):
        raise FileNotFoundError('Post-import verification failed: some referenced objects are not readable.')


def _ensure_multi_repository(backend: t.Any) -> t.Any:
    """Return the profile's repository backend, rebuilding it if it predates the shared repositories registration.

    The storage backend caches its repository instance. If the shared repositories were registered only during this
    process (e.g. through ``aiida-s3 import-shared --register``), the cached repository predates the registration
    and is rebuilt from the (now updated) storage configuration.

    :param backend: the profile's storage backend.
    :raises ValueError: if the repository does not support shared repositories.
    """
    repository = backend.get_repository()

    if not hasattr(repository, 'verify_shared') and backend.profile.storage_config.get('shared_repositories'):
        backend._repository = None
        repository = backend.get_repository()

    if not hasattr(repository, 'verify_shared'):
        raise ValueError('The current profile storage does not support shared repositories.')

    return repository


def _remap_archive_node_keys(db_path: pathlib.Path, name: str) -> None:
    """Namespace every node ``repository_metadata`` object key in the archive database for shared repository ``name``.

    :param db_path: path to the extracted ``db.sqlite3`` archive database.
    :param name: the name under which importing profiles will register the source bucket.
    """
    connection = sqlite3.connect(db_path)

    try:
        rows = connection.execute('SELECT id, repository_metadata FROM db_dbnode').fetchall()
        for node_id, serialized in rows:
            metadata = json.loads(serialized)
            if any(True for _ in iter_repository_keys(metadata)):
                connection.execute(
                    'UPDATE db_dbnode SET repository_metadata = ? WHERE id = ?',
                    (json.dumps(remap_repository_metadata(metadata, name)), node_id),
                )
        connection.commit()
    finally:
        connection.close()


def _collect_archive_node_keys(db_path: pathlib.Path) -> list[str]:
    """Return the sorted set of object keys referenced by the archive database's nodes.

    :param db_path: path to the extracted ``db.sqlite3`` archive database.
    :raises ValueError: if any referenced key is not namespaced for a shared repository.
    """
    connection = sqlite3.connect(db_path)

    try:
        rows = connection.execute('SELECT repository_metadata FROM db_dbnode').fetchall()
    finally:
        connection.close()

    keys = {key for (serialized,) in rows for _, key in iter_repository_keys(json.loads(serialized))}

    unnamespaced = [key for key in keys if split_shared_key(key) is None]
    if unnamespaced:
        raise ValueError(
            f'The archive contains objects that are not namespaced for a shared repository: {unnamespaced}. '
            'Import it with `verdi archive import` instead.'
        )

    return sorted(keys)


@contextlib.contextmanager
def _preserve_archive_keys() -> t.Iterator[None]:
    """Disable the archive key-format conversion of aiida-core's import machinery.

    The reader backend of an archive always reports its key format as ``sha256``, such that importing into a
    ``uuid4`` repository (any S3 backend) triggers a conversion that regenerates every object key. For
    shared-repository archives the keys must be preserved verbatim: they are namespaced routing identifiers, and
    the objects they reference are adopted by reference, not by content.
    """
    import aiida.tools.archive.imports as archive_imports

    # On unpatched versions of ``aiida-core``, the conversion does not exist and there is nothing to bypass.
    original = getattr(archive_imports, 'convert_archive_key_format', None)

    def preserve_archive_keys(key_format_from: str, key_format_to: str, file_path: str, test_run: bool = False) -> str:
        """Return the archive path unchanged: shared-repository archives must never be key-converted."""
        return file_path

    if original is not None:
        archive_imports.convert_archive_key_format = preserve_archive_keys  # type: ignore[attr-defined]

    try:
        yield
    finally:
        if original is not None:
            archive_imports.convert_archive_key_format = original  # type: ignore[attr-defined]
