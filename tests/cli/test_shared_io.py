"""Tests for the :mod:`aiida_s3.shared_io` module, covering the full shared-repository export/import roundtrip.

The test creates two temporary profiles on the same mocked S3 server:

* profile A exports a node as a shared-repository archive;
* profile B registers A's primary bucket as a read-only shared repository and imports the archive.

The central assertion is that B's own bucket receives zero objects and that B's node streams its file content
directly from the shared bucket.
"""

import io
import json
import sqlite3
import typing as t
import uuid
import zipfile

import pytest
from aiida_s3.repository.multi import MultiS3RepositoryBackend, make_shared_key
from aiida_s3.shared import iter_repository_keys
from aiida_s3.shared_io import DATABASE_NAME, METADATA_NAME, export_shared, import_shared


@pytest.fixture
def exporter_config(config_psql_s3) -> dict[str, t.Any]:
    """Return the storage configuration of the exporting profile."""
    return config_psql_s3()


@pytest.fixture
def importer_config(config_psql_s3, s3) -> dict[str, t.Any]:
    """Return the storage configuration of the importing profile.

    The primary bucket is distinct from the exporting profile's bucket, which is registered as the shared
    repository ``common``.
    """
    config = config_psql_s3()
    config['bucket_name'] = f"{s3['bucket_name']}-importer"
    config['shared_repositories'] = [
        {
            'name': 'common',
            'endpoint_url': s3['endpoint_url'],
            'access_key_id': s3['access_key_id'],
            'secret_access_key': s3['secret_access_key'],
            'bucket_name': s3['bucket_name'],
        }
    ]
    return config


def test_export_shared(exporter_config, importer_config, aiida_config, aiida_profile_factory, tmp_path):
    """Test the full shared-repository export/import roundtrip."""
    from aiida import orm

    with aiida_profile_factory(
        aiida_config, storage_backend='s3.psql_s3', storage_config=exporter_config
    ) as profile_exporter:
        node = orm.Dict(dict={'parameter': 1})
        node.base.repository.put_object_from_filelike(io.BytesIO(b'content-a'), 'sub/file_a.txt')
        node.base.repository.put_object_from_filelike(io.BytesIO(b'content-b'), 'file_b.txt')
        node.store()
        node_uuid = node.uuid
        node_keys = dict(iter_repository_keys(node.backend_entity.repository_metadata))

        archive_path = tmp_path / 'common.aiida-shared'
        export_shared('common', [node], archive_path)

        # The exporter's archive contains no object bytes but the manifest.
        with zipfile.ZipFile(archive_path, 'r') as archive:
            assert sorted(archive.namelist()) == sorted([METADATA_NAME, DATABASE_NAME])
            metadata = json.loads(archive.read(METADATA_NAME))
            assert metadata['shared_repository'] == {
                'format': 'aiida-s3-shared/1',
                'name': 'common',
                'endpoint_url': profile_exporter.storage_config['endpoint_url'],
                'bucket_name': profile_exporter.storage_config['bucket_name'],
                'key_format': 'uuid4',
            }

            # The node metadata in the archive database is namespaced.
            db_path = tmp_path / DATABASE_NAME
            db_path.write_bytes(archive.read(DATABASE_NAME))

    connection = sqlite3.connect(db_path)
    try:
        (serialized,) = connection.execute('SELECT repository_metadata FROM db_dbnode').fetchone()
    finally:
        connection.close()

    archive_keys = dict(iter_repository_keys(json.loads(serialized)))
    assert archive_keys == {path: make_shared_key('common', key) for path, key in node_keys.items()}

    with aiida_profile_factory(aiida_config, storage_backend='s3.psql_s3', storage_config=importer_config):
        from aiida.manage import get_manager

        repository = get_manager().get_profile_storage().get_repository()
        assert isinstance(repository, MultiS3RepositoryBackend)
        assert repository.verify_shared() == {'common': True}
        assert list(repository.list_objects()) == []

        import_shared(archive_path)

        # The importing profile's bucket received zero objects.
        assert list(repository.list_objects()) == []

        # The node was imported and references the shared objects by namespaced key.
        node = orm.load_node(node_uuid)
        imported_keys = dict(iter_repository_keys(node.backend_entity.repository_metadata))
        assert imported_keys == archive_keys

        # The content is streamed from the shared bucket.
        assert node.base.repository.get_object_content('sub/file_a.txt', mode='rb') == b'content-a'
        assert node.base.repository.get_object_content('file_b.txt', mode='rb') == b'content-b'

        # The shared keys resolve in the shared repository, the primary bucket stays empty.
        assert all(repository.has_objects(list(imported_keys.values())))
        assert not any(repository._primary.has_objects(list(imported_keys.values())))

        # The original keys exist in the shared bucket (which is the exporter's primary bucket).
        shared_repository = repository._shared['common']
        assert shared_repository.has_objects(list(node_keys.values()))


def test_import_shared_register(  # noqa: PLR0913
    exporter_config, config_psql_s3, s3, aiida_config, aiida_profile_factory, tmp_path
):
    """Test the ``--register`` path: the shared repository is registered at import time from the manifest."""
    from aiida import orm

    # The importing profile starts without any shared repositories registered.
    importer_config = config_psql_s3()
    importer_config['bucket_name'] = f"{s3['bucket_name']}-importer-register"

    with aiida_profile_factory(aiida_config, storage_backend='s3.psql_s3', storage_config=exporter_config):
        node = orm.Dict(dict={'parameter': 1})
        node.base.repository.put_object_from_filelike(io.BytesIO(b'content-a'), 'file_a.txt')
        node.store()
        node_uuid = node.uuid

        archive_path = tmp_path / 'common.aiida-shared'
        export_shared('common', [node], archive_path)

    with aiida_profile_factory(
        aiida_config, storage_backend='s3.psql_s3', storage_config=importer_config
    ) as profile_importer:
        from aiida.manage import get_manager

        assert not profile_importer.storage_config.get('shared_repositories')

        import_shared(
            archive_path,
            credentials={
                'access_key_id': s3['access_key_id'],
                'secret_access_key': s3['secret_access_key'],
            },
        )

        # The registration was persisted in the profile's storage configuration.
        entries = profile_importer.storage_config['shared_repositories']
        assert len(entries) == 1
        assert entries[0]['name'] == 'common'

        node = orm.load_node(node_uuid)
        assert node.base.repository.get_object_content('file_a.txt', mode='rb') == b'content-a'

        repository = get_manager().get_profile_storage().get_repository()
        assert list(repository.list_objects()) == []


def test_import_shared_not_shared_archive(exporter_config, aiida_config, aiida_profile_factory, tmp_path):
    """Test that importing a vanilla archive through ``import_shared`` fails."""
    from aiida import orm
    from aiida.tools.archive.create import create_archive

    with aiida_profile_factory(aiida_config, storage_backend='s3.psql_s3', storage_config=exporter_config):
        node = orm.Dict(dict={'parameter': 1})
        node.base.repository.put_object_from_filelike(io.BytesIO(b'content-a'), 'file_a.txt')
        node.store()

        from aiida.manage import get_manager

        archive_path = tmp_path / 'vanilla.aiida'
        create_archive(
            entities=[node],
            filename=archive_path,
            archive_format=get_manager().get_profile_storage().get_repository().archive_format,
        )

        with pytest.raises(ValueError, match='not a shared-repository archive'):
            import_shared(archive_path)


def test_import_shared_missing_object(exporter_config, importer_config, aiida_config, aiida_profile_factory, tmp_path):
    """Test that importing fails loudly if a referenced object is missing from the shared bucket."""
    from aiida import orm

    with aiida_profile_factory(aiida_config, storage_backend='s3.psql_s3', storage_config=exporter_config):
        node = orm.Dict(dict={'parameter': 1})
        node.base.repository.put_object_from_filelike(io.BytesIO(b'content-a'), 'file_a.txt')
        node.store()

        archive_path = tmp_path / 'common.aiida-shared'
        export_shared('common', [node], archive_path)

        # Sabotage: remove the object from the shared (exporter's) bucket.
        from aiida.manage import get_manager

        repository = get_manager().get_profile_storage().get_repository()
        repository.delete_objects(list(dict(iter_repository_keys(node.backend_entity.repository_metadata)).values()))

    with aiida_profile_factory(aiida_config, storage_backend='s3.psql_s3', storage_config=importer_config):
        with pytest.raises(FileNotFoundError, match='missing from the shared repository'):
            import_shared(archive_path)


def test_export_shared_missing_object(exporter_config, aiida_config, aiida_profile_factory, tmp_path):
    """Test that the pre-export verification fails if a referenced object is missing."""
    from aiida import orm

    with aiida_profile_factory(aiida_config, storage_backend='s3.psql_s3', storage_config=exporter_config):
        node = orm.Dict(dict={'parameter': 1})
        node.base.repository.put_object_from_filelike(io.BytesIO(b'content-a'), 'file_a.txt')
        node.store()

        from aiida.manage import get_manager

        repository = get_manager().get_profile_storage().get_repository()
        repository.delete_objects(list(dict(iter_repository_keys(node.backend_entity.repository_metadata)).values()))

        with pytest.raises(FileNotFoundError, match='missing from the primary bucket'):
            export_shared('common', [node], tmp_path / 'common.aiida-shared')


def test_attach_shared_object(exporter_config, importer_config, aiida_config, aiida_profile_factory):
    """Test attaching an existing shared object to a node of the importing profile."""
    from aiida import orm

    with aiida_profile_factory(aiida_config, storage_backend='s3.psql_s3', storage_config=exporter_config):
        node = orm.Dict(dict={'parameter': 1})
        node.base.repository.put_object_from_filelike(io.BytesIO(b'content-a'), 'file_a.txt')
        node.store()
        object_key = dict(iter_repository_keys(node.backend_entity.repository_metadata))['file_a.txt']

    with aiida_profile_factory(aiida_config, storage_backend='s3.psql_s3', storage_config=importer_config):
        from aiida_s3.shared import attach_shared_object

        node = orm.Dict(dict={'parameter': 2}).store()

        with pytest.raises(FileNotFoundError, match='not found in shared repository'):
            attach_shared_object(node, 'pseudo.txt', 'common', str(uuid.uuid4()))

        attach_shared_object(node, 'sub/pseudo.txt', 'common', object_key)

        assert node.base.repository.get_object_content('sub/pseudo.txt', mode='rb') == b'content-a'
        assert dict(iter_repository_keys(node.backend_entity.repository_metadata))['sub/pseudo.txt'] == (
            make_shared_key('common', object_key)
        )
