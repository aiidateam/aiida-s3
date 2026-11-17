"""Tests for the :mod:`aiida_s3.shared` metadata helpers."""

import pytest
from aiida_s3.repository.multi import make_shared_key
from aiida_s3.shared import iter_repository_keys, remap_repository_metadata


@pytest.fixture
def nested_metadata() -> dict:
    """Return repository metadata with a nested directory structure."""
    return {
        'o': {
            'file_a': {'k': '0e7c9f8a-1111-2222-3333-444455556666'},
            'empty_folder': {},
            'folder': {
                'o': {
                    'file_b': {'k': 'aaaa9f8a-1111-2222-3333-444455556666'},
                },
            },
        }
    }


def test_remap_repository_metadata(nested_metadata):
    """Test that only the ``'k'`` fields are namespaced and the directory structure is preserved."""
    remapped = remap_repository_metadata(nested_metadata, 'common')

    assert remapped == {
        'o': {
            'file_a': {'k': make_shared_key('common', '0e7c9f8a-1111-2222-3333-444455556666')},
            'empty_folder': {},
            'folder': {
                'o': {
                    'file_b': {'k': make_shared_key('common', 'aaaa9f8a-1111-2222-3333-444455556666')},
                },
            },
        }
    }


def test_remap_repository_metadata_no_mutation(nested_metadata):
    """Test that the input metadata is not mutated."""
    import copy

    original = copy.deepcopy(nested_metadata)
    remap_repository_metadata(nested_metadata, 'common')
    assert nested_metadata == original


def test_remap_repository_metadata_idempotent(nested_metadata):
    """Test that already-namespaced keys are re-namespaced, not double-namespaced."""
    remapped = remap_repository_metadata(nested_metadata, 'common')
    remapped = remap_repository_metadata(remapped, 'renamed')

    keys = {key for _, key in iter_repository_keys(remapped)}
    assert keys == {
        make_shared_key('renamed', '0e7c9f8a-1111-2222-3333-444455556666'),
        make_shared_key('renamed', 'aaaa9f8a-1111-2222-3333-444455556666'),
    }


def test_iter_repository_keys(nested_metadata):
    """Test the iteration over the ``(path, key)`` pairs of the file entries."""
    assert dict(iter_repository_keys(nested_metadata)) == {
        'file_a': '0e7c9f8a-1111-2222-3333-444455556666',
        'folder/file_b': 'aaaa9f8a-1111-2222-3333-444455556666',
    }


def test_iter_repository_keys_empty():
    """Test the iteration over empty metadata."""
    assert list(iter_repository_keys({})) == []
    assert list(iter_repository_keys({'o': {}})) == []
