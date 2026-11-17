"""Tests for the :mod:`aiida_s3.storage.psql_s3` storage and ``import_shared_collection``.

Covers the production path of the shared collections: a source profile exports
Data nodes and a Group as a manifest, and a target profile - whose repository
is a ``MultiS3RepositoryBackend`` with the source's bucket registered as a
read-only shared repository - imports it metadata-only. In particular:

* the existence verification of the manifest objects (HEAD-based
  ``has_objects``, batched into one call per import) and its opt-out
  (``verify=False``);
* the single storage transaction around the import: a failure mid-manifest
  persists NOTHING (no partial nodes, no orphan group);
* ``MultiS3RepositoryBackend.has_objects`` routing across the primary and
  shared buckets, preserving the input order.

The S3 layer is mocked with ``moto`` (session fixture from ``conftest.py``);
``region_name='eu-west-1'`` is used throughout because a location constraint is
rejected by moto for ``us-east-1``-style buckets.
"""

from __future__ import annotations

import io
import json
import typing as t
import uuid

import pytest
from aiida import orm
from aiida.manage import get_manager

from aiida_s3.repository.multi import MultiS3RepositoryBackend, make_shared_key
from aiida_s3.repository.s3 import S3RepositoryBackend
from aiida_s3.shared_collection import export_shared_collection, import_shared_collection

SHARED_NAME = 'common'
GROUP_LABEL = 'test-family'
ELEMENTS = ('Si', 'C')


@pytest.fixture
def bucket_names() -> dict[str, str]:
    """Return unique bucket names for the source and target profiles."""
    identifier = str(uuid.uuid4())[:8]
    return {
        'source': f'aiida-s3-test-source-{identifier}',
        'target': f'aiida-s3-test-target-{identifier}',
    }


def make_storage_config(
    config_psql_dos: t.Callable, s3: dict[str, t.Any], bucket_name: str
) -> dict[str, t.Any]:
    """Return a storage configuration for the ``s3.psql_s3`` backend."""
    config = config_psql_dos()
    config.update({
        'endpoint_url': s3['endpoint_url'],
        'access_key_id': s3['access_key_id'],
        'secret_access_key': s3['secret_access_key'],
        'region_name': 'eu-west-1',
        'bucket_name': bucket_name,
    })
    return config


@pytest.fixture
def source_manifest(
    config_psql_dos,
    aiida_config,
    aiida_profile_factory,
    s3,
    bucket_names,
    tmp_path,
) -> tuple:
    """Create a source profile with one Group of SinglefileData nodes and export it as a manifest.

    :return: ``(manifest_path, elements)`` where ``elements`` maps each node to the attribute used to identify it.
    """
    manifest_path = tmp_path / 'manifest.json'

    with aiida_profile_factory(
        aiida_config,
        storage_backend='s3.psql_s3',
        storage_config=make_storage_config(config_psql_dos, s3, bucket_names['source']),
    ):
        backend = get_manager().get_profile_storage()

        nodes = []
        for element in ELEMENTS:
            node = orm.SinglefileData(io.BytesIO(f'content-{element}'.encode()), filename=f'{element}.txt')
            node.base.attributes.set('element', element)
            node.store()
            nodes.append(node)

        group = orm.Group(label=GROUP_LABEL)
        group.store()
        group.add_nodes(nodes)

        export_shared_collection(
            data_nodes=nodes, groups=[group], output=manifest_path, shared_name=SHARED_NAME, backend=backend
        )

    return manifest_path, ELEMENTS


@pytest.fixture
def target_profile(config_psql_dos, aiida_config, aiida_profile_factory, s3, bucket_names):
    """Return a factory opening a target profile with the source bucket registered as a shared repository."""

    def open_target_profile():
        storage_config = make_storage_config(config_psql_dos, s3, bucket_names['target'])
        storage_config['shared_repositories'] = [{
            'name': SHARED_NAME,
            'endpoint_url': s3['endpoint_url'],
            'access_key_id': s3['access_key_id'],
            'secret_access_key': s3['secret_access_key'],
            'region_name': 'eu-west-1',
            'bucket_name': bucket_names['source'],
        }]
        return aiida_profile_factory(aiida_config, storage_backend='s3.psql_s3', storage_config=storage_config)

    return open_target_profile


def test_export_import_roundtrip(source_manifest, target_profile):
    """A metadata-only import recreates nodes and group, referencing the shared objects without copying them."""
    manifest_path, elements = source_manifest

    with target_profile():
        backend = get_manager().get_profile_storage()
        repository = backend.get_repository()
        assert isinstance(repository, MultiS3RepositoryBackend)

        result = import_shared_collection(
            manifest_path=manifest_path, shared_name=SHARED_NAME, backend=backend, verify=True
        )

        assert len(result['data_nodes']) == len(elements)
        imported = {node.base.attributes.get('element'): node for node in result['data_nodes']}
        for element in elements:
            assert imported[element].get_content(mode='rb') == f'content-{element}'.encode()

        [group] = result['groups']
        assert group.label == GROUP_LABEL
        assert {node.uuid for node in group.nodes} == {node.uuid for node in result['uuid_map'].values()}

        # the shared bucket is referenced, never copied into the target's own
        assert not list(repository.list_objects())


def test_import_verify_false_skips_existence_checks(source_manifest, target_profile, monkeypatch):
    """With ``verify=False`` no S3 existence check is made: the manifest keys are trusted as-is."""
    manifest_path, elements = source_manifest

    with target_profile():
        backend = get_manager().get_profile_storage()
        repository = backend.get_repository()

        def fail(keys):
            raise AssertionError('has_objects must not be called when verify=False')

        monkeypatch.setattr(repository, 'has_objects', fail)

        result = import_shared_collection(
            manifest_path=manifest_path, shared_name=SHARED_NAME, backend=backend, verify=False
        )
        assert len(result['data_nodes']) == len(elements)


def test_import_verification_missing_object_fails_upfront(source_manifest, target_profile):
    """With ``verify=True`` a manifest referencing a missing object fails before anything is stored."""
    manifest_path, elements = source_manifest
    manifest = json.loads(manifest_path.read_text())
    manifest['data_nodes'][0]['repository_files'][0]['s3_key'] = 'missing-key'
    broken_path = manifest_path.with_name('broken-missing.json')
    broken_path.write_text(json.dumps(manifest))

    with target_profile():
        backend = get_manager().get_profile_storage()

        with pytest.raises(FileNotFoundError, match='missing-key'):
            import_shared_collection(
                manifest_path=broken_path, shared_name=SHARED_NAME, backend=backend, verify=True
            )

        assert orm.QueryBuilder().append(orm.Data).count() == 0
        assert orm.QueryBuilder().append(orm.Group, filters={'label': GROUP_LABEL}).count() == 0


def test_import_failure_rolls_back_everything(source_manifest, target_profile):
    """A failure mid-manifest persists nothing: the whole import is a single transaction."""
    manifest_path, elements = source_manifest
    manifest = json.loads(manifest_path.read_text())
    # one valid node followed by an unloadable node type: the failure occurs
    # AFTER the first node was stored, inside the transaction
    manifest['data_nodes'].append({
        'source_uuid': str(uuid.uuid4()),
        'node_type': 'data.does.not.exist',
        'repository_files': [],
        'attributes': {},
        'extras': {},
    })
    broken_path = manifest_path.with_name('broken-node-type.json')
    broken_path.write_text(json.dumps(manifest))

    with target_profile():
        backend = get_manager().get_profile_storage()

        with pytest.raises(ValueError, match='Cannot load node type'):
            import_shared_collection(
                manifest_path=broken_path, shared_name=SHARED_NAME, backend=backend, verify=True
            )

        assert orm.QueryBuilder().append(orm.Data).count() == 0
        assert orm.QueryBuilder().append(orm.Group, filters={'label': GROUP_LABEL}).count() == 0


def test_has_objects_routes_and_preserves_order(s3, bucket_names):
    """``has_objects`` resolves each key with a HEAD request, routing by namespace and preserving the input order."""

    def backend_for(bucket: str) -> S3RepositoryBackend:
        return S3RepositoryBackend(
            endpoint_url=s3['endpoint_url'],
            access_key_id=s3['access_key_id'],
            secret_access_key=s3['secret_access_key'],
            bucket_name=bucket,
            region_name='eu-west-1',
        )

    primary = backend_for(bucket_names['target'])
    shared = backend_for(bucket_names['source'])
    primary.initialise()
    shared.initialise()

    shared_keys = [shared.put_object_from_filelike(io.BytesIO(f'obj-{i}'.encode())) for i in range(3)]
    primary_key = primary.put_object_from_filelike(io.BytesIO(b'primary'))

    multi = MultiS3RepositoryBackend(primary, {SHARED_NAME: shared})
    namespaced = [make_shared_key(SHARED_NAME, key) for key in shared_keys]

    # more than one key per call exercises the concurrent HEAD resolution
    assert multi.has_objects(namespaced + [make_shared_key(SHARED_NAME, 'missing')]) == [True, True, True, False]
    assert multi.has_objects([primary_key, namespaced[0], primary_key]) == [True, True, True]
    assert primary.has_objects([primary_key, 'missing']) == [True, False]
