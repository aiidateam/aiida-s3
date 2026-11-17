"""Generic export/import of Data nodes and Group subclasses as shared S3 collections.

This module provides a simple, provenance-free way to share Data nodes (with their
repository files) and Group subclasses between AiiDA profiles. The shared objects
are referenced by key in a read-only S3 bucket - no data is duplicated.

Manifest format:
    {
        "format": "aiida-s3-shared-collection/1",
        "source": {"endpoint_url": ..., "bucket_name": ...},
        "data_nodes": [
            {
                "source_uuid": ...,
                "node_type": "pseudo.upf",  # entry point name
                "attributes": {...},
                "extras": {...},
                "label": ...,
                "repository_files": [
                    {"path": "Si.UPF", "s3_key": "uuid-...", "size_bytes": ..., "sha256": ...}
                ]
            }
        ],
        "groups": [
            {
                "source_uuid": ...,
                "group_type": "pseudo.family.sssp",  # entry point name
                "label": "SSSP/1.3/PBE/efficiency",
                "description": ...,
                "extras": {...},  # includes cutoffs, etc.
                "member_source_uuids": ["uuid1", "uuid2"]  # references to data_nodes
            }
        ]
    }

Usage:
    # Export a pseudo family
    export_shared_collection(
        data_nodes=[pseudo1, pseudo2, ...],
        groups=[family],
        output=Path("family.json"),
        shared_name="common"
    )

    # Import into another profile
    import_shared_collection(
        manifest_path=Path("family.json"),
        shared_name="common"
    )

    # Import into an explicitly given backend, without any globally loaded profile
    # (e.g. the DashboardServer temporal worker, which manages its own aiida
    # Managers/backends and must not touch global state):
    import_shared_collection(
        manifest_path=Path("family.json"),
        shared_name="common",
        backend=my_backend_instance,
    )
"""

from __future__ import annotations

import hashlib
import json
import logging
import pathlib
import typing as t

if t.TYPE_CHECKING:
    from aiida.orm.implementation.storage_backend import StorageBackend

__all__ = (
    'COLLECTION_FORMAT',
    'export_shared_collection',
    'import_shared_collection',
    'iter_repository_keys',
)

COLLECTION_FORMAT = 'aiida-s3-shared-collection/1'

logger = logging.getLogger(__name__)


def _pseudo_base_class(node_class: type) -> type | None:
    """Return the aiida-pseudo base class in the MRO of ``node_class``, if any.

    The base class overrides ``store()`` to re-validate the md5 by re-reading
    the file content - ``PseudoPotential`` in aiida-pseudo < 1.10,
    ``PseudoPotentialData`` from 1.10 on (same pattern, renamed class).
    """
    from aiida_pseudo.data import pseudo as pseudo_module

    for name in ('PseudoPotentialData', 'PseudoPotential'):
        pseudo_base = getattr(pseudo_module, name, None)
        if pseudo_base is None:
            continue
        if any(cls is pseudo_base for cls in node_class.__mro__):
            return pseudo_base
    return None


def _store_node(node: t.Any, node_class: type) -> t.Any:
    """Store an imported node, bypassing the aiida-pseudo md5 re-validation.

    ``aiida-pseudo``'s ``store()`` calls ``validate_md5()``, which downloads the
    full file content from the repository backend only to recompute a checksum.
    For a metadata-only import that re-downloads every shared object - the exact
    cost the import exists to avoid (profiling a 103-node SSSP fixture: 86% of
    the wall time, all of it in ``validate_md5`` -> S3 GET) - and it re-proves
    what cannot have changed: the ``md5`` attribute comes from the export
    manifest, which recorded it from these very objects, and the shared bucket
    is read-only (its objects are checked up front when ``verify=True``, and a
    missing one would raise on first open regardless).

    The bypass invokes the parent implementation directly: no monkey-patching
    of shared class state (the import runs on worker threads alongside other
    activities in the same process) and aiida-core's own ``store()`` checks
    (storability, links, caching, value cleaning) still run. The validating
    override is skipped only for pseudo nodes; every other class stores
    normally.
    """
    pseudo_base = _pseudo_base_class(node_class)
    if pseudo_base is not None:
        return super(pseudo_base, node).store()
    return node.store()


def iter_repository_keys(metadata: dict[str, t.Any], prefix: str = '') -> t.Iterator[tuple[str, str]]:
    """Yield ``(path, key)`` tuples for every file entry in node repository metadata."""
    if 'k' in metadata:
        yield prefix, metadata['k']
    for name, child in metadata.get('o', {}).items():
        yield from iter_repository_keys(child, f'{prefix}/{name}' if prefix else name)


def _get_entry_point_name(obj: t.Any) -> str:
    """Return the entry point name for a Data or Group class."""
    if hasattr(obj, 'get_entry_point_name'):
        return obj.get_entry_point_name()
    # Fallback: get from class
    cls = obj if isinstance(obj, type) else obj.__class__
    from aiida.plugins.entry_point import get_entry_point_from_class
    _, entry_point = get_entry_point_from_class(cls.__module__, cls.__name__)
    return entry_point.name


def export_shared_collection(
    data_nodes: t.Iterable[t.Any],
    groups: t.Iterable[t.Any] | None = None,
    output: pathlib.Path | str = 'shared_collection.json',
    shared_name: str = 'shared',
    verify: bool = True,
    backend: StorageBackend | None = None,
) -> pathlib.Path:
    """Export Data nodes and Groups as a shared collection manifest.

    The manifest contains all metadata needed to recreate the nodes in another
    profile, with repository files referenced by key in the source S3 bucket.

    :param data_nodes: iterable of Data nodes to export.
    :param groups: iterable of Group nodes to export (with their members).
    :param output: path to write the manifest JSON file.
    :param shared_name: name under which the source bucket will be registered
        as a shared repository in the importing profile.
    :param verify: whether to verify that all repository objects exist in S3.
    :param backend: storage backend of the profile owning the nodes. If not specified, the storage of the globally
        loaded profile is used, which is the natural mode for CLI usage (e.g. in an aiida pod, where the profile is
        loaded globally). Pass it explicitly when no profile is loaded globally, such as in a service process managing
        several profiles at once.
    :return: the path to the created manifest file.
    """
    from aiida import orm
    from aiida.manage import get_manager

    output = pathlib.Path(output)
    backend = backend or get_manager().get_profile_storage()
    repository = backend.get_repository()
    profile = backend.profile

    data_nodes = list(data_nodes)
    groups = list(groups or [])

    # Collect all S3 keys for verification
    all_keys: list[str] = []

    manifest: dict[str, t.Any] = {
        'format': COLLECTION_FORMAT,
        'shared_name': shared_name,
        'source': {
            'endpoint_url': profile.storage_config.get('endpoint_url'),
            'bucket_name': profile.storage_config['bucket_name'],
        },
        'data_nodes': [],
        'groups': [],
    }

    # Export data nodes
    for node in data_nodes:
        if not isinstance(node, orm.Data):
            raise TypeError(f'Expected Data node, got {type(node)}')
        if not node.is_stored:
            raise ValueError(f'Node {node} is not stored')

        # Get repository files
        repository_files = []
        metadata = node.backend_entity.repository_metadata or {}
        for path, key in iter_repository_keys(metadata):
            all_keys.append(key)
            file_info: dict[str, t.Any] = {
                'path': path,
                's3_key': key,
            }
            # Get file size and hash
            try:
                with repository.open(key) as handle:
                    content = handle.read()
                file_info['size_bytes'] = len(content)
                file_info['sha256'] = hashlib.sha256(content).hexdigest()
            except Exception:
                pass  # Size/hash optional
            repository_files.append(file_info)

        manifest['data_nodes'].append({
            'source_uuid': node.uuid,
            'node_type': _get_entry_point_name(node),
            'attributes': dict(node.base.attributes.all),
            'extras': dict(node.base.extras.all),
            'label': node.label or '',
            'description': node.description or '',
            'repository_files': repository_files,
        })

    # Verify all objects exist
    if verify and all_keys:
        missing = [key for key, exists in zip(all_keys, repository.has_objects(all_keys)) if not exists]
        if missing:
            raise FileNotFoundError(f'Objects missing from S3 bucket: {missing}')

    # Export groups
    for group in groups:
        if not isinstance(group, orm.Group):
            raise TypeError(f'Expected Group, got {type(group)}')
        if not group.is_stored:
            raise ValueError(f'Group {group} is not stored')

        # Get member UUIDs (only Data nodes that are in our export set)
        exported_uuids = {node.uuid for node in data_nodes}
        member_source_uuids = [
            node.uuid for node in group.nodes
            if node.uuid in exported_uuids
        ]

        manifest['groups'].append({
            'source_uuid': group.uuid,
            'group_type': _get_entry_point_name(group),
            'label': group.label,
            'description': group.description or '',
            'extras': dict(group.base.extras.all),
            'member_source_uuids': member_source_uuids,
        })

    output.write_text(json.dumps(manifest, indent=2))
    return output


def import_shared_collection(
    manifest_path: pathlib.Path | str,
    shared_name: str | None = None,
    credentials: dict[str, str] | None = None,
    label_prefix: str = '',
    backend: StorageBackend | None = None,
    verify: bool = True,
) -> dict[str, t.Any]:
    """Import a shared collection manifest into a profile.

    Creates new Data nodes and Groups, with repository files referenced by key
    in the shared S3 bucket. No data is uploaded or downloaded.

    All nodes and groups are stored within a single storage transaction: one
    commit for the whole manifest. If anything fails, nothing is persisted,
    so an interrupted or failed import leaves no partial nodes behind.

    :param manifest_path: path to the manifest JSON file.
    :param shared_name: name of the shared repository. If None, uses the name
        from the manifest.
    :param credentials: optional credentials to register the shared repository
        in the profile configuration of the globally loaded profile. Requires
        a globally loaded profile and is therefore incompatible with an
        explicit ``backend``.
    :param label_prefix: optional prefix to add to group labels (to avoid conflicts).
    :param backend: storage backend to import into. If not specified, the storage of the globally loaded profile is
        used, which is the natural mode for CLI usage (e.g. in an aiida pod, where the profile is loaded globally).
        Pass it explicitly when no profile is loaded globally, such as in a service process managing several
        profiles at once. The shared repository must already be registered in the profile's storage configuration
        (``shared_repositories``) in this mode: registering it requires writing the global profile configuration.
    :param verify: whether to verify that all manifest objects exist in the shared bucket before importing (one
        HEAD request per object). The shared bucket is read-only and was verified at export time, and a missing
        object raises ``FileNotFoundError`` when the file is first opened, so this is an early-failure check
        rather than a correctness requirement: callers that import frequently may skip it.
    :return: dict with ``data_nodes`` and ``groups`` lists of imported entities and the ``uuid_map`` mapping the
        manifest source UUIDs to the imported nodes.
    """
    from aiida import orm
    from aiida.manage import get_manager
    from aiida.plugins import DataFactory, GroupFactory
    from aiida_s3.shared import add_shared_repository
    from aiida_s3.repository.multi import make_shared_key, MultiS3RepositoryBackend

    manifest_path = pathlib.Path(manifest_path)
    manifest = json.loads(manifest_path.read_text())

    if manifest.get('format') != COLLECTION_FORMAT:
        raise ValueError(f'Not a shared collection manifest: {manifest_path}')

    shared_name = shared_name or manifest.get('shared_name', 'shared')

    # Register shared repository if credentials provided
    if credentials:
        if backend is not None:
            raise ValueError(
                '`credentials` cannot be combined with an explicit `backend`: registering a shared repository '
                'writes the configuration of the globally loaded profile. Pass `backend=None` (and load the '
                'profile globally), or register the shared repository in the storage configuration of the '
                'profile behind `backend` beforehand.'
            )
        add_shared_repository(
            name=shared_name,
            endpoint_url=manifest['source']['endpoint_url'],
            access_key_id=credentials['access_key_id'],
            secret_access_key=credentials['secret_access_key'],
            bucket_name=manifest['source']['bucket_name'],
        )

    backend = backend or get_manager().get_profile_storage()
    repository = backend.get_repository()

    # Ensure we have a multi-repository backend
    if not isinstance(repository, MultiS3RepositoryBackend):
        if backend.profile.storage_config.get('shared_repositories'):
            # Rebuild repository
            backend._repository = None
            repository = backend.get_repository()
        if not isinstance(repository, MultiS3RepositoryBackend):
            raise ValueError('Profile does not support shared repositories (use s3.psql_s3 storage)')

    # Verify shared repository is accessible
    if not repository.verify_shared().get(shared_name, False):
        raise ValueError(
            f"Shared repository '{shared_name}' not registered or not reachable. "
            "Register it with 'aiida-s3 shared add' or pass credentials."
        )

    # Map source UUID -> imported node
    uuid_map: dict[str, orm.Data] = {}
    imported_data_nodes: list[orm.Data] = []
    imported_groups: list[orm.Group] = []

    # Optionally verify every shared object exists. ``has_objects`` resolves
    # each key with a single HEAD request, so this costs one request per
    # object of the manifest (independent of the bucket size). It is an
    # early-failure check, not a correctness requirement: the shared bucket
    # is read-only and was verified at export time, and a missing object
    # raises ``FileNotFoundError`` when the file is first opened. With
    # ``verify=False`` the manifest keys are trusted as-is.
    all_shared_keys = {
        make_shared_key(shared_name, file_info['s3_key'])
        for node_data in manifest['data_nodes']
        for file_info in node_data.get('repository_files', [])
    }
    if not verify:
        existing_shared_keys = all_shared_keys
    else:
        existing_shared_keys: set[str] = set()
        if all_shared_keys:
            for shared_key, exists in zip(
                all_shared_keys, repository.has_objects(sorted(all_shared_keys))
            ):
                if exists:
                    existing_shared_keys.add(shared_key)

    # Import everything in a single storage transaction: node stores, group
    # stores and group-membership inserts only flush to the savepoint and
    # the manifest commits exactly once. Besides turning one fsync per node
    # into one for the whole import, this makes the import atomic: a failure
    # or a hard kill (activity timeout, worker restart) leaves NOTHING
    # persisted, so a retry starts from a clean profile instead of
    # duplicating the nodes of every unfinished fixture.
    with backend.transaction():
        # Import data nodes
        for node_data in manifest['data_nodes']:
            source_uuid = node_data['source_uuid']
            node_type = node_data['node_type']

            # Load the node class
            try:
                node_class = DataFactory(node_type)
            except Exception as e:
                raise ValueError(f'Cannot load node type {node_type}: {e}')

            # Build shared repository metadata first
            metadata: dict[str, t.Any] = {'o': {}}
            shared_keys: list[str] = []
            for file_info in node_data.get('repository_files', []):
                path = file_info['path']
                s3_key = file_info['s3_key']
                shared_key = make_shared_key(shared_name, s3_key)

                # Verified up front when verify=True (see has_objects above);
                # with verify=False the manifest keys are trusted as-is
                if shared_key not in existing_shared_keys:
                    raise FileNotFoundError(
                        f"Object '{s3_key}' not found in shared bucket '{shared_name}'"
                    )

                shared_keys.append(shared_key)

                # Build nested metadata structure
                parts = [p for p in path.split('/') if p]
                current = metadata['o']
                for part in parts[:-1]:
                    current = current.setdefault(part, {}).setdefault('o', {})
                if parts:
                    current[parts[-1]] = {'k': shared_key}

            # No data transfer: the manifest already carries everything the
            # exported node had computed (the parsed metadata lives in the
            # attributes) and the shared keys were checked up front (when
            # ``verify=True``). The node
            # is created EMPTY and its repository metadata is pre-armed with
            # the shared keys: with a non-sandbox repository backend,
            # NodeRepository._store() skips the sandbox clone entirely and
            # store() persists the metadata directly - one DB insert, zero S3
            # requests per node.
            # The previous strategy (read the content so the constructor could
            # re-parse it, store a placeholder copy in the primary bucket, then
            # redirect the metadata and delete the placeholder) moved the full
            # file three times per node and redid the parsing the source
            # profile had already done: a metadata-only import cannot afford
            # that, an import of ~2500 pseudos took minutes instead of seconds.
            node = node_class(backend=backend)

            # Set attributes (the parsed metadata of the exported node)
            for key, value in node_data.get('attributes', {}).items():
                node.base.attributes.set(key, value)

            # Set extras
            for key, value in node_data.get('extras', {}).items():
                node.base.extras.set(key, value)

            # Set label/description
            if node_data.get('label'):
                node.label = node_data['label']
            if node_data.get('description'):
                node.description = node_data['description']

            # Pre-arm the repository with the shared keys: no content is read
            # or written, the node simply references the shared objects
            if shared_keys:
                from aiida.repository import Repository

                node.base.repository._repository = Repository.from_serialized(
                    backend=node.backend.get_repository(), serialized=metadata
                )

            # Store the node: with the pre-armed repository the metadata goes
            # straight into the backend node row (no placeholder upload), and the
            # md5 re-validation is skipped (see _store_node): re-hashing the file
            # would re-download it from the shared bucket
            _store_node(node, node_class)

            uuid_map[source_uuid] = node
            imported_data_nodes.append(node)

        # Import groups
        for group_data in manifest['groups']:
            group_type = group_data['group_type']

            # Load the group class
            try:
                group_class = GroupFactory(group_type)
            except Exception as e:
                raise ValueError(f'Cannot load group type {group_type}: {e}')

            # Create new group
            label = label_prefix + group_data['label'] if label_prefix else group_data['label']
            group = group_class(label=label, backend=backend)
            if group_data.get('description'):
                group.description = group_data['description']

            # Set extras
            for key, value in group_data.get('extras', {}).items():
                group.base.extras.set(key, value)

            # Store the group
            group.store()

            # Add members (only those that were exported)
            members = [
                uuid_map[uuid] for uuid in group_data.get('member_source_uuids', [])
                if uuid in uuid_map
            ]
            if members:
                group.add_nodes(members)

            imported_groups.append(group)

    return {
        'data_nodes': imported_data_nodes,
        'groups': imported_groups,
        'uuid_map': uuid_map,
    }
