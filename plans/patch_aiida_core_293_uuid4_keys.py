#!/usr/bin/env python
"""Port the khsrali aiida-core archive patch (uuid4 key preservation) onto aiida-core 2.9.3.

Applies to the throwaway test venv at /tmp/aiida293-test only. Reference: the patched aiida-core
2.6.3.post0 in /home/bertossa/MaterysMinikube_eni/.venv (fork commit 48eefe446 and friends).
"""

import pathlib
import shutil
import sys

PATCHED = pathlib.Path('/home/bertossa/MaterysMinikube_eni/.venv/lib/python3.13/site-packages/aiida')
TARGET = pathlib.Path('/tmp/aiida293-test/lib/python3.14/site-packages/aiida')


def patch(path, replacements, require_all=True):
    """Apply a list of (old, new) exact-string replacements to the target file."""
    filepath = TARGET / path
    text = filepath.read_text()
    for old, new in replacements:
        if old not in text:
            if require_all:
                raise RuntimeError(f'Pattern not found in {path}:\n{old}')
            continue
        if text.count(old) != 1:
            raise RuntimeError(f'Pattern not unique in {path}:\n{old}')
        text = text.replace(old, new)
    filepath.write_text(text)
    print(f'patched {path}')


# ---------------------------------------------------------------- repository/backend/abstract.py
patch(
    'repository/backend/abstract.py',
    [
        (
            '''        with the repository).
        """

    @abc.abstractmethod
    def initialise(self, **kwargs: Any) -> None:''',
            '''        with the repository).
        """

    @property
    def archive_format(self) -> 'ArchiveFormatAbstract':
        """Return the format for the archive of exported repository.

        The format is bound to the repository backend, since the object keys of an archive must match the key
        format of the storage that will import it.
        """
        from aiida.tools.archive.implementations.sqlite_zip import ArchiveFormatSqlUUIDZip, ArchiveFormatSqlZip

        if self.key_format == 'uuid4':
            return ArchiveFormatSqlUUIDZip()
        return ArchiveFormatSqlZip()

    @abc.abstractmethod
    def initialise(self, **kwargs: Any) -> None:''',
        ),
        (
            '''    def put_object_from_filelike(self, handle: BinaryIO) -> str:
        """Store the byte contents of a file in the repository.

        :param handle: filelike object with the byte content to be stored.
        :return: the generated fully qualified identifier for the object within the repository.
        :raises TypeError: if the handle is not a byte stream.
        """
        if not isinstance(handle, io.BufferedIOBase) and not self.is_readable_byte_stream(handle):
            raise TypeError(f'handle does not seem to be a byte stream: {type(handle)}.')
        return self._put_object_from_filelike(handle)

    @abc.abstractmethod
    def _put_object_from_filelike(self, handle: BinaryIO) -> str:
        pass''',
            '''    def put_object_from_filelike(self, handle: BinaryIO, key: str | None = None) -> str:
        """Store the byte contents of a file in the repository.

        :param handle: filelike object with the byte content to be stored.
        :param key: fully qualified identifier for the object within the repository. If not provided, a new
            key will be generated.
        :return: the generated fully qualified identifier for the object within the repository.
        :raises TypeError: if the handle is not a byte stream.
        """
        if not isinstance(handle, io.BufferedIOBase) and not self.is_readable_byte_stream(handle):
            raise TypeError(f'handle does not seem to be a byte stream: {type(handle)}.')
        return self._put_object_from_filelike(handle, key)

    @abc.abstractmethod
    def _put_object_from_filelike(self, handle: BinaryIO, key: str | None = None) -> str:
        pass''',
        ),
    ],
)

# ---------------------------------------------------------------- repository/backend/sandbox.py
patch(
    'repository/backend/sandbox.py',
    [
        (
            '''    def _put_object_from_filelike(self, handle: t.BinaryIO) -> str:
        """Store the byte contents of a file in the repository.

        :param handle: filelike object with the byte content to be stored.
        :return: the generated fully qualified identifier for the object within the repository.
        :raises TypeError: if the handle is not a byte stream.
        """
        key = str(uuid.uuid4())''',
            '''    def _put_object_from_filelike(self, handle: t.BinaryIO, key: str | None = None) -> str:
        """Store the byte contents of a file in the repository.

        :param handle: filelike object with the byte content to be stored.
        :param key: fully qualified identifier for the object within the repository. If not provided, a new
            key will be generated.
        :return: the generated fully qualified identifier for the object within the repository.
        :raises TypeError: if the handle is not a byte stream.
        """
        if key is None:
            key = str(uuid.uuid4())''',
        ),
    ],
)

# ---------------------------------------------------------------- repository/backend/disk_object_store.py
patch(
    'repository/backend/disk_object_store.py',
    [
        (
            '''    def _put_object_from_filelike(self, handle: t.BinaryIO) -> str:
        """Store the byte contents of a file in the repository.

        :param handle: filelike object with the byte content to be stored.
        :return: the generated fully qualified identifier for the object within the repository.
        :raises TypeError: if the handle is not a byte stream.
        """''',
            '''    def _put_object_from_filelike(self, handle: t.BinaryIO, key: str | None = None) -> str:
        """Store the byte contents of a file in the repository.

        :param handle: filelike object with the byte content to be stored.
        :param key: ignored, since this backend generates its own keys.
        :return: the generated fully qualified identifier for the object within the repository.
        :raises TypeError: if the handle is not a byte stream.
        """''',
        ),
    ],
)

# ---------------------------------------------------------------- storage/sqlite_zip/backend.py
patch(
    'storage/sqlite_zip/backend.py',
    [
        (
            '''    @property
    def key_format(self) -> str | None:
        return 'sha256'

    def initialise(self, **kwargs: Any) -> None:''',
            '''    @property
    def key_format(self) -> str | None:
        return 'sha256'

    @property
    def archive_format(self) -> Any:
        """Return the format for the archive of exported repository."""
        return super().archive_format

    def initialise(self, **kwargs: Any) -> None:''',
        ),
    ],
)

# ---------------------------------------------------------------- tools/archive/conversion.py
shutil.copyfile(PATCHED / 'tools/archive/conversion.py', TARGET / 'tools/archive/conversion.py')
print('copied tools/archive/conversion.py')

# ---------------------------------------------------------------- tools/archive/imports.py
patch(
    'tools/archive/imports.py',
    [
        (
            'from .common import entity_type_to_orm',
            'from .common import entity_type_to_orm\nfrom .conversion import convert_archive_key_format',
        ),
        (
            '''    with archive_format.open(path, mode='r') as reader:
        backend_from = reader.get_backend()

        # To ensure we do not corrupt the backend database on a faulty import,''',
            '''    with archive_format.open(path, mode='r') as reader:
        backend_from = reader.get_backend()
        converted_archive_path = convert_archive_key_format(
            backend_from.get_repository().archive_format.key_format,
            backend.get_repository().archive_format.key_format,
            path,
            test_run,
        )

    with archive_format.open(converted_archive_path, mode='r') as reader:
        backend_from = reader.get_backend()

        # To ensure we do not corrupt the backend database on a faulty import,''',
        ),
        (
            '''        for key, handle in repository_from.iter_object_streams(new_keys):
            backend_key = repository_to.put_object_from_filelike(handle)''',
            '''        for key, handle in repository_from.iter_object_streams(new_keys):
            if repository_to.archive_format.maintain_keys:
                backend_key = repository_to.put_object_from_filelike(handle, key=key)
            else:
                backend_key = repository_to.put_object_from_filelike(handle)''',
        ),
    ],
)

# ---------------------------------------------------------------- sqlite_zip/main.py
main = TARGET / 'tools/archive/implementations/sqlite_zip/main.py'
text = main.read_text()
old = '''    @property
    def key_format(self) -> str:
        return 'sha256'
'''
new = '''    @property
    def key_format(self) -> str:
        return 'sha256'

    @property
    def maintain_keys(self) -> bool:
        return False
'''
assert text.count(old) == 1, 'ArchiveFormatSqlZip.key_format block not found'
text = text.replace(old, new)
text += '''

class ArchiveFormatSqlUUIDZip(ArchiveFormatSqlZip):
    """Archive format, which uses a zip file, containing an SQLite database.

    The content of the zip file is::

        |- archive.zip
            |- metadata.json
            |- db.sqlite3
            |- repo/
                |- uuid

    Repository files are named by their UUID.

    """

    @property
    def key_format(self) -> str:
        return 'uuid4'

    @property
    def maintain_keys(self) -> bool:
        return True
'''
main.write_text(text)
print('patched tools/archive/implementations/sqlite_zip/main.py')

print('ALL PATCHES APPLIED')
