"""Tests for the ``aiida-s3`` command line interface."""

import io

from click.testing import CliRunner


def test_shared_commands(config_psql_s3, s3, aiida_config, aiida_profile_factory):
    """Test the ``aiida-s3 shared list`` and ``aiida-s3 shared remove`` commands."""
    from aiida_s3.cli import cmd_root

    config = config_psql_s3()
    config['shared_repositories'] = [
        {
            'name': 'common',
            'endpoint_url': s3['endpoint_url'],
            'access_key_id': s3['access_key_id'],
            'secret_access_key': s3['secret_access_key'],
            'bucket_name': s3['bucket_name'],
        }
    ]

    with aiida_profile_factory(aiida_config, storage_backend='s3.psql_s3', storage_config=config):
        result = CliRunner().invoke(cmd_root, ['shared', 'list'])
        assert result.exit_code == 0, result.output
        assert 'common' in result.output
        assert s3['bucket_name'] in result.output

        result = CliRunner().invoke(cmd_root, ['shared', 'remove', '--name', 'common'])
        assert result.exit_code == 0, result.output
        assert 'Success: removed shared repository `common`.' in result.output

        result = CliRunner().invoke(cmd_root, ['shared', 'list'])
        assert result.exit_code == 0, result.output
        assert 'No shared repositories registered.' in result.output


def test_export_import_shared(config_psql_s3, s3, aiida_config, aiida_profile_factory, tmp_path):
    """Test the ``aiida-s3 export-shared`` and ``aiida-s3 import-shared --register`` commands."""
    from aiida import orm
    from aiida_s3.cli import cmd_root

    # A profile without any shared repositories registered: the import registers it from the manifest.
    config = config_psql_s3()

    with aiida_profile_factory(aiida_config, storage_backend='s3.psql_s3', storage_config=config):
        node = orm.Dict(dict={'parameter': 1})
        node.base.repository.put_object_from_filelike(io.BytesIO(b'content'), 'file.txt')
        node.store()
        node_uuid = node.uuid

        archive_path = tmp_path / 'common.aiida-shared'
        result = CliRunner().invoke(cmd_root, ['export-shared', '--name', 'common', '--all', '-o', str(archive_path)])
        assert result.exit_code == 0, result.output
        assert archive_path.exists()

        result = CliRunner().invoke(
            cmd_root,
            ['import-shared', str(archive_path), '--register'],
            input=f"{s3['access_key_id']}\n{s3['secret_access_key']}\n",
        )
        assert result.exit_code == 0, result.output

        node = orm.load_node(node_uuid)
        assert node.base.repository.get_object_content('file.txt', mode='rb') == b'content'
