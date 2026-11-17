"""Tests for the ``aiida-s3 collection`` CLI commands."""

from __future__ import annotations

import json
import typing as t

import pytest
from click.testing import CliRunner

from aiida_s3.cli import cmd_root


@pytest.fixture
def runner() -> CliRunner:
    """Return a Click CLI runner."""
    return CliRunner()


def test_collection_export_help(runner: CliRunner) -> None:
    """Test that the collection export command shows help."""
    result = runner.invoke(cmd_root, ['collection', 'export', '--help'])
    assert result.exit_code == 0
    assert 'Export Data nodes and Groups as a shared collection manifest' in result.output


def test_collection_import_help(runner: CliRunner) -> None:
    """Test that the collection import command shows help."""
    result = runner.invoke(cmd_root, ['collection', 'import', '--help'])
    assert result.exit_code == 0
    assert 'Import a shared collection manifest into the current profile' in result.output


def test_collection_export_no_input(runner: CliRunner) -> None:
    """Test that collection export fails with no input specified."""
    result = runner.invoke(cmd_root, ['collection', 'export', '-n', 'test', '-o', 'out.json'])
    assert result.exit_code != 0
    assert 'Nothing to export' in result.output


def test_collection_import_invalid_manifest(runner: CliRunner, tmp_path) -> None:
    """Test that collection import fails with an invalid manifest."""
    # Create an invalid manifest
    manifest_path = tmp_path / 'invalid.json'
    manifest_path.write_text(json.dumps({'format': 'wrong-format'}))

    result = runner.invoke(cmd_root, ['collection', 'import', str(manifest_path)])
    assert result.exit_code != 0
    assert 'not a shared collection manifest' in result.output


def test_collection_export_import_roundtrip(
    runner: CliRunner,
    psql_s3_profile,
    tmp_path,
) -> None:
    """Test a full export/import roundtrip via the CLI (with mocked S3)."""
    from aiida import orm

    # Create test data in the profile
    import io
    node1 = orm.Dict(dict={'key': 'value1'})
    node1.store()

    node2 = orm.SinglefileData(io.BytesIO(b'test content'), filename='test.txt')
    node2.store()

    group = orm.Group(label='test-cli-group')
    group.store()
    group.add_nodes([node1, node2])

    # Export via CLI
    manifest_path = tmp_path / 'collection.json'
    result = runner.invoke(cmd_root, [
        'collection', 'export',
        '-n', 'cli-test',
        '-o', str(manifest_path),
        '-G', 'test-cli-group',
    ])
    assert result.exit_code == 0, f'Export failed: {result.output}'
    assert manifest_path.exists()

    # Verify manifest content
    manifest = json.loads(manifest_path.read_text())
    assert manifest['format'] == 'aiida-s3-shared-collection/1'
    assert len(manifest['data_nodes']) == 2
    assert len(manifest['groups']) == 1
    assert manifest['groups'][0]['label'] == 'test-cli-group'

    # Note: Full import test requires a second profile with shared repository,
    # which is covered by the integration tests in test_shared_collection.py
    print('CLI roundtrip export test passed!')


def test_collection_export_specific_nodes(
    runner: CliRunner,
    psql_s3_profile,
    tmp_path,
) -> None:
    """Test exporting specific nodes by PK and UUID."""
    from aiida import orm

    import io
    node1 = orm.Dict(dict={'a': 1})
    node1.store()

    node2 = orm.Dict(dict={'b': 2})
    node2.store()

    manifest_path = tmp_path / 'specific.json'

    # Export by PK
    result = runner.invoke(cmd_root, [
        'collection', 'export',
        '-n', 'specific-test',
        '-o', str(manifest_path),
        '-d', str(node1.pk),
    ])
    assert result.exit_code == 0, f'Export failed: {result.output}'

    manifest = json.loads(manifest_path.read_text())
    assert len(manifest['data_nodes']) == 1
    assert manifest['data_nodes'][0]['attributes'] == {'a': 1}

    # Export by UUID
    manifest_path2 = tmp_path / 'specific2.json'
    result = runner.invoke(cmd_root, [
        'collection', 'export',
        '-n', 'specific-test',
        '-o', str(manifest_path2),
        '-D', node2.uuid,
    ])
    assert result.exit_code == 0, f'Export failed: {result.output}'

    manifest2 = json.loads(manifest_path2.read_text())
    assert len(manifest2['data_nodes']) == 1
    assert manifest2['data_nodes'][0]['attributes'] == {'b': 2}

    print('CLI specific nodes test passed!')
