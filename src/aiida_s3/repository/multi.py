"""Composite repository backend routing object access across one read-write and N read-only S3 backends."""

from __future__ import annotations

import contextlib
import typing as t

from aiida.repository.backend.abstract import AbstractRepositoryBackend

from .s3 import S3RepositoryBackend

__all__ = ('MultiS3RepositoryBackend', 'SHARED_PREFIX', 'make_shared_key', 'split_shared_key')

SHARED_PREFIX = 'shared'


def split_shared_key(key: str) -> tuple[str, str] | None:
    """Split ``shared/<name>/<object-key>`` into ``(<name>, <object-key>)``.

    :param key: fully qualified identifier for an object within the repository.
    :return: the tuple of shared repository name and backend-local object key, or ``None`` if the key is not
        namespaced under the shared prefix.
    """
    prefix = f'{SHARED_PREFIX}/'
    if not key.startswith(prefix):
        return None
    name, separator, object_key = key[len(prefix) :].partition('/')
    if not (name and separator and object_key):
        return None
    return name, object_key


def make_shared_key(name: str, object_key: str) -> str:
    """Return the namespaced key for ``object_key`` in the shared repository registered as ``name``.

    :param name: the name under which the shared repository is registered in the profile.
    :param object_key: the backend-local (original) key of the object in the shared bucket.
    """
    return f'{SHARED_PREFIX}/{name}/{object_key}'


class MultiS3RepositoryBackend(AbstractRepositoryBackend):
    """Repository spanning one read-write primary backend and any number of read-only shared backends.

    Keys are routed by namespace:

    * ``shared/<name>/<object-key>``: the object lives in the shared backend registered as ``<name>`` under its
      *original* key. The prefix is stripped before any request reaches S3, such that shared buckets are never
      written to, read from with modified keys, or re-keyed.
    * any other key: the object lives in the primary (read-write) backend.

    The central invariant: no S3 upload, download or key modification ever happens against a shared bucket. Shared
    objects are referenced by key; the bytes live only in the shared bucket, once.

    Deletion of shared objects is controlled by ``soft_delete_shared``:

    * ``soft_delete_shared=True`` (default): shared keys are silently skipped during deletion. The database record
      is removed, but the S3 object in the shared bucket is preserved for other profiles. This allows nodes
      referencing shared data to be deleted normally.
    * ``soft_delete_shared=False``: attempting to delete a shared key raises ``PermissionError``. Use this when
      shared objects must be strictly protected from any deletion attempt.
    """

    def __init__(
        self,
        primary: S3RepositoryBackend,
        shared: dict[str, S3RepositoryBackend] | None = None,
        soft_delete_shared: bool = True,
    ):
        """Construct a new instance.

        :param primary: the read-write backend for all objects owned by this profile.
        :param shared: mapping of shared repository name to read-only backend.
        :param soft_delete_shared: if ``True`` (default), deletion of shared-namespaced keys is silently skipped
            (the S3 object in the shared bucket is preserved). If ``False``, deletion of shared keys raises
            ``PermissionError``.
        """
        self._primary = primary
        self._shared = dict(shared or {})
        self._soft_delete_shared = soft_delete_shared

    def __str__(self) -> str:
        """Return the string representation of this repository."""
        names = ','.join(sorted(self._shared))
        return f'MultiS3RepositoryBackend: <{self._primary}> shared: <{names}>'

    def _route(self, key: str) -> tuple[AbstractRepositoryBackend, str]:
        """Return the ``(backend, backend-local key)`` for a fully qualified key.

        :param key: fully qualified identifier for an object within the repository.
        :raises KeyError: if the key is namespaced for a shared repository that is not registered.
        """
        parts = split_shared_key(key)
        if parts is None:
            return self._primary, key
        name, object_key = parts
        try:
            return self._shared[name], object_key
        except KeyError:
            raise KeyError(f'No shared repository registered under name `{name}`.') from None

    # ------------------------------------------------------------------ identity

    @property
    def uuid(self) -> str:
        """Return the unique identifier of the repository, which is that of the primary backend."""
        return self._primary.uuid

    @property
    def key_format(self) -> str | None:
        """Return the format for the keys of the repository.

        This is declared as ``uuid4``: the namespaced shared keys are routable identifiers, while the underlying
        keys are UUID4. The format must remain ``uuid4`` such that archive imports reuse, rather than regenerate,
        the keys of imported objects.
        """
        return 'uuid4'

    @property
    def archive_format(self) -> t.Any:
        """Return the format for the archive of exported repository."""
        return super().archive_format

    # ------------------------------------------------------------------ lifecycle

    @property
    def is_initialised(self) -> bool:
        """Return whether the repository has been initialised.

        Only the primary bucket needs to exist; shared buckets are pre-provisioned and read-only.
        """
        return self._primary.is_initialised

    def initialise(self, **kwargs: t.Any) -> None:
        """Initialise the primary repository if needed. Shared buckets are never created or modified."""
        self._primary.initialise(**kwargs)

    def verify_shared(self) -> dict[str, bool]:
        """Return whether each registered shared bucket exists and is reachable.

        :return: mapping of shared repository name to whether its bucket is reachable.
        """
        return {name: backend.is_initialised for name, backend in self._shared.items()}

    def erase(self) -> None:
        """Erase the primary repository. Shared buckets are untouchable."""
        self._primary.erase()

    # ------------------------------------------------------------------ put

    def _put_object_from_filelike(self, handle: t.BinaryIO, key: str | None = None) -> str:
        """Store an object in the primary backend, or adopt a shared object by reference.

        :param handle: filelike object with the byte content to be stored.
        :param key: fully qualified identifier for the object within the repository. If not provided, a new key
            will be generated. If the key is namespaced for a shared repository, no byte transfer occurs: the
            handle is discarded, the object's existence in the shared bucket is verified, and the key is returned
            as-is. This is the code path taken by ``aiida-s3 import-shared`` and archive imports.
        :return: the generated fully qualified identifier for the object within the repository.
        :raises FileNotFoundError: if a shared-namespaced key does not exist in the shared bucket.
        """
        if key is None:
            return self._primary._put_object_from_filelike(handle)

        if split_shared_key(key) is not None:
            backend, object_key = self._route(key)
            if not backend.has_objects([object_key])[0]:
                raise FileNotFoundError(f'Object with key `{key}` does not exist in the shared repository.')
            return key

        return self._primary._put_object_from_filelike(handle, key)

    # ------------------------------------------------------------------ read

    @contextlib.contextmanager
    def open(self, key: str) -> t.Iterator[t.IO[bytes]]:  # type: ignore[override]
        """Open a file handle to an object stored under the given key.

        :param key: fully qualified identifier for the object within the repository.
        :return: yield a byte stream object.
        :raise FileNotFoundError: if the file does not exist.
        :raise OSError: if the file could not be opened.
        """
        backend, object_key = self._route(key)
        with backend.open(object_key) as handle:
            yield handle

    def iter_object_streams(  # type: ignore[override]
        self, keys: list[str]
    ) -> t.Iterator[tuple[str, t.IO[bytes]]]:
        """Return an iterator over the (read-only) byte streams of objects identified by key.

        :param keys: fully qualified identifiers for the objects within the repository.
        :return: an iterator over the object byte streams.
        :raise FileNotFoundError: if the file does not exist.
        :raise OSError: if a file could not be opened.
        """
        for key in keys:
            with self.open(key) as handle:
                yield key, handle

    def has_objects(self, keys: list[str]) -> list[bool]:
        """Return whether the repository has an object with the given key.

        :param keys: list of fully qualified identifiers for objects within the repository.
        :return: list of booleans, in the same order as the keys provided, with value True if the respective object
            exists and False otherwise.
        """
        # Group the keys per backend, so each backend is only queried once for all of its keys.
        grouped: dict[AbstractRepositoryBackend, list[tuple[str, str]]] = {}
        for key in keys:
            backend, object_key = self._route(key)
            grouped.setdefault(backend, []).append((key, object_key))

        results: dict[str, bool] = {}
        for backend, items in grouped.items():
            exists = backend.has_objects([object_key for _, object_key in items])
            for (key, _), object_exists in zip(items, exists):
                results[key] = object_exists

        return [results[key] for key in keys]

    # ------------------------------------------------------------------ delete / list / maintain

    def delete_objects(self, keys: list[str]) -> None:
        """Delete the objects from the repository.

        Shared-namespaced keys (``shared/<name>/<object-key>``) are handled according to ``soft_delete_shared``:

        * if ``soft_delete_shared`` is ``True`` (default), shared keys are silently filtered out: the S3 objects
          in the shared buckets are preserved, only the database records are removed by the caller.
        * if ``soft_delete_shared`` is ``False``, any shared key in the list raises ``PermissionError``.

        :param keys: list of fully qualified identifiers for the objects within the repository.
        :raises PermissionError: if ``soft_delete_shared`` is ``False`` and any key is namespaced for a shared
            repository.
        """
        import logging

        logger = logging.getLogger(__name__)

        primary_keys: list[str] = []
        for key in keys:
            if split_shared_key(key) is not None:
                if not self._soft_delete_shared:
                    raise PermissionError(
                        f'Cannot delete object in shared repository: {key}. '
                        'Shared repositories are read-only (soft_delete_shared=False).'
                    )
                logger.info('Soft-deleting shared object %s (object preserved in shared bucket)', key)
            else:
                primary_keys.append(key)

        if primary_keys:
            self._primary.delete_objects(primary_keys)

    def list_objects(self) -> t.Iterable[str]:
        """Return iterable that yields all available objects by key.

        Only objects owned by this profile are listed: shared objects are not owned by any profile.
        """
        yield from self._primary.list_objects()

    def maintain(self, **kwargs: t.Any) -> dict[t.Any, t.Any]:
        """Perform maintenance operations on the primary repository only. Shared buckets are never touched.

        :return: a dictionary with information on the operations performed.
        """
        return self._primary.maintain(**kwargs)

    def get_info(self, detailed: bool = False, **kwargs: t.Any) -> dict[t.Any, t.Any]:
        """Return information on configuration and content of the repository."""
        info = self._primary.get_info(detailed, **kwargs)
        info['shared_repositories'] = {
            name: {'uuid': backend.uuid, 'initialised': backend.is_initialised}
            for name, backend in self._shared.items()
        }
        return info
