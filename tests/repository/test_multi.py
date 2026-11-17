"""Tests for the :mod:`aiida_s3.repository.multi` module."""

import io
import typing as t
import uuid

import pytest
from aiida_s3.repository.multi import MultiS3RepositoryBackend, make_shared_key, split_shared_key
from aiida_s3.repository.s3 import S3RepositoryBackend


@pytest.fixture
def shared_bucket_name(s3_bucket_name: str) -> str:
    """Return the name of a second bucket, distinct from the primary session bucket."""
    return f'{s3_bucket_name}-shared'


@pytest.fixture
def shared_repository(s3_config, shared_bucket_name) -> t.Generator[S3RepositoryBackend, None, None]:
    """Return an initialised backend for the shared bucket, cleaning it up at the end."""
    repository = S3RepositoryBackend(bucket_name=shared_bucket_name, **s3_config)
    repository.initialise()
    yield repository
    repository.erase()


@pytest.fixture
def primary_repository(s3_bucket_name, s3_config) -> S3RepositoryBackend:
    """Return a backend for the primary session bucket (created and cleaned up by the ``s3`` fixture)."""
    return S3RepositoryBackend(bucket_name=s3_bucket_name, **s3_config)


@pytest.fixture
def repository(primary_repository, shared_repository) -> MultiS3RepositoryBackend:
    """Return an initialised multi backend with one shared repository registered as ``common``.

    Uses the default ``soft_delete_shared=True``: shared keys are silently skipped during deletion.
    """
    return MultiS3RepositoryBackend(primary_repository, {'common': shared_repository})


@pytest.fixture
def repository_strict(primary_repository, shared_repository) -> MultiS3RepositoryBackend:
    """Return a multi backend with ``soft_delete_shared=False``: shared key deletion raises."""
    return MultiS3RepositoryBackend(primary_repository, {'common': shared_repository}, soft_delete_shared=False)


def test_split_shared_key():
    """Test the :func:`aiida_s3.repository.multi.split_shared_key` function."""
    assert split_shared_key('0e7c9f8a-1111-2222-3333-444455556666') is None
    assert split_shared_key('shared') is None
    assert split_shared_key('shared/') is None
    assert split_shared_key('shared/common') is None
    assert split_shared_key('shared//0e7c') is None
    assert split_shared_key('shared/common/0e7c9f8a-1111-2222-3333-444455556666') == (
        'common',
        '0e7c9f8a-1111-2222-3333-444455556666',
    )
    # Subdirectories in the shared bucket are supported: only the first two segments are consumed.
    assert split_shared_key('shared/common/sub/0e7c') == ('common', 'sub/0e7c')


def test_make_shared_key():
    """Test the :func:`aiida_s3.repository.multi.make_shared_key` function."""
    assert make_shared_key('common', '0e7c') == 'shared/common/0e7c'
    assert split_shared_key(make_shared_key('common', '0e7c')) == ('common', '0e7c')


def test_str(repository):
    """Test the ``__str__`` of the multi backend."""
    assert 'common' in str(repository)


def test_uuid_and_key_format(repository, s3_bucket_name):
    """Test that the identity of the multi backend is that of the primary backend."""
    assert repository.uuid == s3_bucket_name
    assert repository.key_format == 'uuid4'
    assert repository.archive_format.key_format == 'uuid4'
    assert repository.archive_format.maintain_keys


def test_is_initialised_and_initialise(repository, shared_repository):
    """Test that initialisation only concerns the primary bucket."""
    assert repository.is_initialised
    # Both buckets exist at this point.
    assert shared_repository.is_initialised
    repository.erase()
    # Erasing the multi backend erases the primary bucket only.
    assert not repository.is_initialised
    assert shared_repository.is_initialised
    repository.initialise()
    assert repository.is_initialised


def test_verify_shared(repository, s3_config, shared_bucket_name):
    """Test the :meth:`aiida_s3.repository.multi.MultiS3RepositoryBackend.verify_shared` method."""
    assert repository.verify_shared() == {'common': True}

    # A shared repository whose bucket does not exist is reported as unreachable.
    repository._shared['missing'] = S3RepositoryBackend(bucket_name=f'{shared_bucket_name}-missing', **s3_config)
    assert repository.verify_shared() == {'common': True, 'missing': False}


def test_put_object_new_key(repository):
    """Test that new objects (no explicit key) are written to the primary bucket."""
    with io.BytesIO(b'primary') as handle:
        key = repository.put_object_from_filelike(handle)

    assert split_shared_key(key) is None
    assert repository.has_object(key)
    assert key in list(repository.list_objects())


def test_put_object_explicit_key_primary(repository):
    """Test that an explicit non-namespaced key is written to the primary bucket."""
    key = str(uuid.uuid4())
    with io.BytesIO(b'content') as handle:
        assert repository.put_object_from_filelike(handle, key=key) == key
    assert repository.has_object(key)


def test_put_object_explicit_key_shared_no_byte_transfer(repository, shared_repository):
    """Test that an explicit shared-namespaced key adopts the object by reference, without byte transfer."""
    primary_keys = set(repository.list_objects())
    with io.BytesIO(b'shared-bytes') as handle:
        object_key = shared_repository.put_object_from_filelike(handle)

    namespaced_key = make_shared_key('common', object_key)

    with io.BytesIO(b'placeholder') as handle:
        assert repository.put_object_from_filelike(handle, key=namespaced_key) == namespaced_key

    # No object was added to the primary bucket and the shared object is untouched.
    assert set(repository.list_objects()) == primary_keys
    with shared_repository.open(object_key) as handle:
        assert handle.read() == b'shared-bytes'


def test_put_object_explicit_key_shared_missing(repository):
    """Test that adopting a shared key that does not exist raises."""
    namespaced_key = make_shared_key('common', str(uuid.uuid4()))
    with io.BytesIO(b'placeholder') as handle:
        with pytest.raises(FileNotFoundError):
            repository.put_object_from_filelike(handle, key=namespaced_key)


def test_put_object_explicit_key_unknown_shared_name(repository):
    """Test that a namespaced key for an unregistered shared repository raises."""
    namespaced_key = make_shared_key('unknown', str(uuid.uuid4()))
    with io.BytesIO(b'placeholder') as handle:
        with pytest.raises(KeyError):
            repository.put_object_from_filelike(handle, key=namespaced_key)


def test_open_and_read_routing(repository, shared_repository, primary_repository):
    """Test that ``open`` routes by namespace and that shared keys are stripped before reaching S3."""
    with io.BytesIO(b'shared-bytes') as handle:
        shared_object_key = shared_repository.put_object_from_filelike(handle)
    with io.BytesIO(b'primary-bytes') as handle:
        primary_object_key = primary_repository.put_object_from_filelike(handle)

    with repository.open(make_shared_key('common', shared_object_key)) as handle:
        assert handle.read() == b'shared-bytes'
    with repository.open(primary_object_key) as handle:
        assert handle.read() == b'primary-bytes'

    # The namespaced key never reaches S3: the shared bucket does not contain it.
    assert not shared_repository.has_object(make_shared_key('common', shared_object_key))
    assert not shared_repository.has_object(f'shared/common/{shared_object_key}')


def test_iter_object_streams(repository, shared_repository):
    """Test the iteration over object streams of both backends."""
    with io.BytesIO(b'shared-bytes') as handle:
        shared_object_key = shared_repository.put_object_from_filelike(handle)
    with io.BytesIO(b'primary-bytes') as handle:
        primary_object_key = repository.put_object_from_filelike(handle)

    keys = [make_shared_key('common', shared_object_key), primary_object_key]
    streams = {key: handle.read() for key, handle in repository.iter_object_streams(keys)}
    assert streams == {
        make_shared_key('common', shared_object_key): b'shared-bytes',
        primary_object_key: b'primary-bytes',
    }


def test_has_objects(repository, shared_repository):
    """Test that ``has_objects`` routes by namespace and preserves the order of the input."""
    with io.BytesIO(b'shared-bytes') as handle:
        shared_object_key = shared_repository.put_object_from_filelike(handle)
    with io.BytesIO(b'primary-bytes') as handle:
        primary_object_key = repository.put_object_from_filelike(handle)

    keys = [
        make_shared_key('common', shared_object_key),
        primary_object_key,
        make_shared_key('common', str(uuid.uuid4())),
        str(uuid.uuid4()),
    ]

    assert repository.has_objects(keys) == [True, True, False, False]


def test_has_objects_unknown_shared_name(repository):
    """Test that ``has_objects`` raises for a key namespaced for an unregistered shared repository."""
    with pytest.raises(KeyError):
        repository.has_objects([make_shared_key('unknown', str(uuid.uuid4()))])


def test_delete_objects_shared_soft(repository, shared_repository):
    """Test that with ``soft_delete_shared=True`` (default), shared keys are silently skipped.

    The S3 object in the shared bucket is preserved; only the primary deletion proceeds.
    """
    with io.BytesIO(b'shared-bytes') as handle:
        shared_object_key = shared_repository.put_object_from_filelike(handle)
    with io.BytesIO(b'primary-bytes') as handle:
        primary_object_key = repository.put_object_from_filelike(handle)

    shared_key = make_shared_key('common', shared_object_key)

    # Deleting a mix of shared and primary keys: shared is skipped, primary is deleted.
    repository.delete_objects([shared_key, primary_object_key])

    # The shared object is preserved in the shared bucket.
    assert shared_repository.has_object(shared_object_key)
    # The primary object is deleted.
    assert not repository.has_object(primary_object_key)


def test_delete_objects_shared_only_soft(repository, shared_repository):
    """Test that deleting only shared keys with ``soft_delete_shared=True`` is a no-op for S3."""
    with io.BytesIO(b'shared-bytes') as handle:
        shared_object_key = shared_repository.put_object_from_filelike(handle)

    shared_key = make_shared_key('common', shared_object_key)

    # Should not raise, and the shared object must be preserved.
    repository.delete_objects([shared_key])
    assert shared_repository.has_object(shared_object_key)


def test_delete_objects_shared_raises(repository_strict, shared_repository):
    """Test that with ``soft_delete_shared=False``, deleting shared keys raises ``PermissionError``."""
    with io.BytesIO(b'shared-bytes') as handle:
        object_key = shared_repository.put_object_from_filelike(handle)

    with pytest.raises(PermissionError, match='soft_delete_shared=False'):
        repository_strict.delete_objects([make_shared_key('common', object_key)])

    # The shared object is preserved.
    assert shared_repository.has_object(object_key)


def test_delete_objects_primary(repository, shared_repository):
    """Test that deleting primary objects works and never touches the shared bucket."""
    with io.BytesIO(b'primary-bytes') as handle:
        object_key = repository.put_object_from_filelike(handle)
    with io.BytesIO(b'shared-bytes') as handle:
        shared_object_key = shared_repository.put_object_from_filelike(handle)

    repository.delete_objects([object_key])

    assert not repository.has_object(object_key)
    assert shared_repository.has_object(shared_object_key)


def test_list_objects_and_maintain_shared_untouched(repository, shared_repository):
    """Test that ``list_objects`` and ``maintain`` only consider the primary bucket."""
    with io.BytesIO(b'primary-bytes') as handle:
        primary_object_key = repository.put_object_from_filelike(handle)
    with io.BytesIO(b'shared-bytes') as handle:
        shared_object_key = shared_repository.put_object_from_filelike(handle)

    primary_keys = set(repository.list_objects())
    assert primary_object_key in primary_keys
    assert shared_object_key not in primary_keys

    repository.maintain(dry_run=True, live=True)

    # The shared bucket was not touched by either operation.
    assert list(shared_repository.list_objects()) == [shared_object_key]


def test_get_info(repository, shared_repository, s3_bucket_name, shared_bucket_name):
    """Test that ``get_info`` includes the shared repositories."""
    info = repository.get_info()
    assert info['shared_repositories'] == {
        'common': {
            'uuid': shared_bucket_name,
            'initialised': True,
        }
    }


def test_no_shared_repositories(s3_bucket_name, s3_config):
    """Test that a multi backend without shared repositories behaves like the primary backend."""
    primary = S3RepositoryBackend(bucket_name=s3_bucket_name, **s3_config)
    repository = MultiS3RepositoryBackend(primary)

    assert str(repository) == f'MultiS3RepositoryBackend: <S3RepositoryBackend: <{s3_bucket_name}>> shared: <>'

    with io.BytesIO(b'content') as handle:
        key = repository.put_object_from_filelike(handle)

    assert repository.has_object(key)
    assert key in list(repository.list_objects())

    with repository.open(key) as handle:
        assert handle.read() == b'content'

    repository.delete_objects([key])
    assert not repository.has_object(key)


def test_construct_repository_soft_delete_shared_default():
    """Test that ``construct_repository`` defaults to ``soft_delete_shared=True``."""
    from aiida_s3.storage.psql_s3 import construct_repository

    config = {
        'endpoint_url': 'http://localhost:9000',
        'access_key_id': 'test',
        'secret_access_key': 'test',
        'bucket_name': 'primary-bucket',
        'shared_repositories': [
            {
                'name': 'common',
                'endpoint_url': 'http://localhost:9000',
                'access_key_id': 'test',
                'secret_access_key': 'test',
                'bucket_name': 'shared-bucket',
            }
        ],
    }

    repo = construct_repository(config)
    assert isinstance(repo, MultiS3RepositoryBackend)
    assert repo._soft_delete_shared is True


def test_construct_repository_soft_delete_shared_false():
    """Test that ``construct_repository`` passes ``soft_delete_shared=False`` when configured."""
    from aiida_s3.storage.psql_s3 import construct_repository

    config = {
        'endpoint_url': 'http://localhost:9000',
        'access_key_id': 'test',
        'secret_access_key': 'test',
        'bucket_name': 'primary-bucket',
        'soft_delete_shared': False,
        'shared_repositories': [
            {
                'name': 'common',
                'endpoint_url': 'http://localhost:9000',
                'access_key_id': 'test',
                'secret_access_key': 'test',
                'bucket_name': 'shared-bucket',
            }
        ],
    }

    repo = construct_repository(config)
    assert isinstance(repo, MultiS3RepositoryBackend)
    assert repo._soft_delete_shared is False


def test_construct_repository_no_shared():
    """Test that ``construct_repository`` returns a plain S3 backend when no shared repositories are configured."""
    from aiida_s3.storage.psql_s3 import construct_repository

    config = {
        'endpoint_url': 'http://localhost:9000',
        'access_key_id': 'test',
        'secret_access_key': 'test',
        'bucket_name': 'primary-bucket',
    }

    repo = construct_repository(config)
    assert isinstance(repo, S3RepositoryBackend)
    assert not isinstance(repo, MultiS3RepositoryBackend)
