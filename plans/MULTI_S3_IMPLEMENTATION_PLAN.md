# Implementation Plan: Multi-S3 Repository (shared, read-only object store)

Target: `aiida-core 2.9.3` (the version this branch supports — the archive-import machinery
reuses object keys via the explicit `key` parameter of `_put_object_from_filelike`, which was
added upstream exactly for this purpose; see commit `4870c06`).

## 1. Goal and design recap

A profile gets, next to its read-write primary bucket, any number of **read-only shared
repositories** (S3 buckets) that many separated profiles can reference. The central invariant:

> **No S3 upload, download, or key modification ever happens against a shared bucket.**
> Shared objects are referenced by key; the bytes live only in the shared bucket, once.

This is achieved without touching `aiida-core` and without changing the `repository_metadata`
schema:

* `repository_metadata` (JSONB in `db_dbnode`) keeps its aiida-core-owned format
  (`{'o': {…}}` for directories, `{'k': <key>}` for files).
* Object keys are **namespaced**: `shared/<name>/<object-key>` routes to the shared backend
  registered as `<name>`; the prefix is **stripped before any S3 call**, so objects keep their
  original `uuid4` keys in the shared bucket.
* All routing lives in one class: `MultiS3RepositoryBackend`.

```
node.repository_metadata: {"o": {}, "f": ...}          db_dbnode (postgres)
        │  file entry {"k": "shared/common/0e7c…"}
        ▼
MultiS3RepositoryBackend.open("shared/common/0e7c…")  ← strips "shared/common/"
        ▼
S3RepositoryBackend("common").open("0e7c…")           ← boto3 GetObject, read-only
```

## 2. Files touched

| File | Status | Content |
|------|--------|---------|
| `src/aiida_s3/repository/multi.py` | new | `MultiS3RepositoryBackend` (all routing logic) |
| `src/aiida_s3/storage/psql_s3.py` | modified (~10 lines) | `get_repository()` builds the composite when `shared_repositories` is configured |
| `src/aiida_s3/shared.py` | new | config helpers + metadata remap + attach API |
| `src/aiida_s3/shared_io.py` | new | archive export/import internals |
| `src/aiida_s3/cli.py` | new | `aiida-s3` CLI (`export-shared`, `import-shared`, `shared add/list/remove`) |
| `pyproject.toml` | unchanged | `aiida-s3 = "aiida_s3.cli:cmd_root"` is already declared (line 52) but currently dangling — this plan fills it in |
| `tests/repository/test_multi.py`, `tests/cli/test_shared_io.py` | new | see §8 |

Total new code: roughly 500–600 lines, of which ~180 are the backend itself. No `aiida-core`
changes, no database migration, no entry-point additions.

## 3. `MultiS3RepositoryBackend` (`src/aiida_s3/repository/multi.py`)

```python
"""Composite repository backend routing object access across one read-write and N read-only S3 backends."""

from __future__ import annotations

import contextlib
import typing as t

from aiida.repository.backend.abstract import AbstractRepositoryBackend

from .s3 import S3RepositoryBackend

__all__ = ('MultiS3RepositoryBackend', 'SHARED_PREFIX', 'split_shared_key')

SHARED_PREFIX = 'shared'


def split_shared_key(key: str) -> tuple[str, str] | None:
    """Split ``shared/<name>/<object-key>`` into ``(<name>, <object-key>)``.

    Return ``None`` if the key is not a namespaced shared key.
    """
    if not key.startswith(f'{SHARED_PREFIX}/'):
        return None
    _, name, object_key = key.split('/', 2)
    return name, object_key


def make_shared_key(name: str, object_key: str) -> str:
    """Return the namespaced key for ``object_key`` in the shared repository ``name``."""
    return f'{SHARED_PREFIX}/{name}/{object_key}'


class MultiS3RepositoryBackend(AbstractRepositoryBackend):
    """Repository spanning one read-write primary backend and N read-only shared backends.

    Keys are routed by namespace:

    * ``shared/<name>/<object-key>``: object lives in the shared backend registered as ``<name>``
      under its *original* key. The prefix is stripped before any request reaches S3, so shared
      buckets are never written to, read from with modified keys, or re-keyed.
    * any other key: object lives in the primary (read-write) backend.
    """

    def __init__(self, primary: S3RepositoryBackend, shared: dict[str, S3RepositoryBackend] | None = None):
        self._primary = primary
        self._shared = dict(shared or {})

    def __str__(self) -> str:
        names = ','.join(sorted(self._shared))
        return f'MultiS3RepositoryBackend: <{self._primary}> shared: <{names}>'

    def _route(self, key: str) -> tuple[AbstractRepositoryBackend, str]:
        """Return ``(backend, backend-local key)`` for a namespaced key."""
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
        """The repository UUID is that of the primary backend (bucket name)."""
        return self._primary.uuid

    @property
    def key_format(self) -> str | None:
        """Declared as ``uuid4``: the namespaced keys are routable, the underlying keys are UUID4.

        This must remain ``uuid4`` so that archive imports reuse (do not regenerate) keys.
        """
        return 'uuid4'

    @property
    def archive_format(self) -> str | None:
        return super().archive_format

    # ------------------------------------------------------------------ lifecycle

    @property
    def is_initialised(self) -> bool:
        """Only the primary bucket must exist; shared buckets are pre-provisioned and read-only."""
        return self._primary.is_initialised

    def initialise(self, **kwargs: t.Any) -> None:
        """Create the primary bucket if needed. Shared buckets are *never* created or modified."""
        self._primary.initialise(**kwargs)

    def verify_shared(self) -> dict[str, bool]:
        """Return per-name existence of the shared buckets (used by the CLI to fail fast)."""
        return {name: backend.is_initialised for name, backend in self._shared.items()}

    def erase(self) -> None:
        """Erase the primary bucket only. Shared buckets are untouchable."""
        self._primary.erase()

    # ------------------------------------------------------------------ put

    def _put_object_from_filelike(self, handle: t.BinaryIO, key: str | None = None) -> str:
        """Store an object in the primary backend, or adopt a shared object by reference.

        * ``key is None``: new UUID4 key, written to the primary bucket.
        * shared-namespaced ``key``: **no byte transfer**. The handle is discarded, the object's
          existence in the shared bucket is verified, and the key is returned as-is. This is the
          code path taken by ``aiida-s3 import-shared`` and archive imports.
        * other explicit ``key``: written to the primary bucket under that key (archive import of
          non-shared objects).
        """
        if key is None:
            return self._primary._put_object_from_filelike(handle)

        if split_shared_key(key) is not None:
            backend, object_key = self._route(key)
            if not backend.has_objects([object_key])[0]:
                raise FileNotFoundError(f'Object with key `{key}` does not exist in the shared repository.')
            return key

        return self._primary._put_object_from_filelike(handle, key)

    def put_object_from_filelike(self, handle: t.BinaryIO, key: str | None = None) -> str:
        """Override the abstract method: the base implementation would validate the returned key
        against the declared ``key_format`` (``uuid4``), which namespaced shared keys are not.
        Shared keys are validated by existence in the shared bucket instead.
        """
        key = self._put_object_from_filelike(handle, key)
        if split_shared_key(key) is None and self.key_format is not None and not self.validate_key(key):
            raise ValueError(f'Object generated an invalid key: `{key}`.')
        return key

    # ------------------------------------------------------------------ read

    @contextlib.contextmanager
    def open(self, key: str) -> t.Iterator[t.IO[bytes]]:  # type: ignore[override]
        backend, object_key = self._route(key)
        with backend.open(object_key) as handle:
            yield handle

    def iter_object_streams(self, keys: list[str]) -> t.Iterator[tuple[str, t.IO[bytes]]]:
        for key in keys:
            with self.open(key) as handle:
                yield key, handle

    def has_objects(self, keys: list[str]) -> list[bool]:
        results: dict[str, bool] = {}
        for key in keys:
            backend, object_key = self._route(key)
            results[key] = backend.has_objects([object_key])[0]
        return [results[key] for key in keys]

    # ------------------------------------------------------------------ delete / list / maintain

    def delete_objects(self, keys: list[str]) -> None:
        shared_keys = [key for key in keys if split_shared_key(key) is not None]
        if shared_keys:
            raise PermissionError(
                'Cannot delete objects in shared repositories: '
                f'{shared_keys}. Shared repositories are read-only.'
            )
        self._primary.delete_objects(keys)

    def list_objects(self) -> t.Iterable[str]:
        """List only objects owned by this profile. Shared objects are not owned by any profile."""
        yield from self._primary.list_objects()

    def maintain(self, **kwargs: t.Any) -> dict[t.Any, t.Any]:
        """Maintenance runs on the primary bucket only; shared buckets are never touched."""
        return self._primary.maintain(**kwargs)

    def get_info(self, detailed: bool = False, **kwargs: t.Any) -> dict[t.Any, t.Any]:
        info = self._primary.get_info(detailed, **kwargs)
        info['shared_repositories'] = {
            name: {'uuid': backend.uuid, 'initialised': backend.is_initialised}
            for name, backend in self._shared.items()
        }
        return info
```

Notes:

* `delete_objects` raising `PermissionError` is the in-process guard; the *real* guarantee is
  IAM/bucket policy on the shared bucket (credentials with `s3:GetObject` only).
* `list_objects` / `erase` / `maintain` deliberately ignore shared buckets: a profile must never
  reason about shared objects as its own. GC of the shared bucket is an admin concern (§7).

## 4. Profile configuration

Shared repositories live in the profile's `storage.config` under an optional
`shared_repositories` key (managed by the CLI in §6, **not** by `verdi profile setup` — nested
lists do not prompt well and credentials are per-profile concerns anyway):

```yaml
storage:
  backend: s3.psql_s3
  config:
    database_hostname: ...
    endpoint_url: http://localhost:9000
    access_key_id: ...
    secret_access_key: ...
    bucket_name: my-profile-bucket
    shared_repositories:                       # OPTIONAL — absent = plain single-backend profile
      - name: common-data
        endpoint_url: http://minio.internal:9000
        access_key_id: read-only-key          # IAM: s3:GetObject on the shared bucket only
        secret_access_key: ...
        bucket_name: shared-bucket
```

The only modification to existing code — `src/aiida_s3/storage/psql_s3.py`,
`PsqlS3StorageMigrator.get_repository` (currently line 43) gains a construction step:

```python
from ..repository.multi import MultiS3RepositoryBackend
from ..repository.s3 import S3RepositoryBackend


def _construct_repository(storage_config: dict) -> S3RepositoryBackend | MultiS3RepositoryBackend:
    """Construct the file repository backend from the profile storage config."""
    primary = S3RepositoryBackend(
        endpoint_url=storage_config['endpoint_url'],
        access_key_id=storage_config['access_key_id'],
        secret_access_key=storage_config['secret_access_key'],
        bucket_name=storage_config['bucket_name'],
    )

    shared_config = storage_config.get('shared_repositories')

    if not shared_config:
        return primary

    shared = {
        config['name']: S3RepositoryBackend(
            endpoint_url=config['endpoint_url'],
            access_key_id=config['access_key_id'],
            secret_access_key=config['secret_access_key'],
            bucket_name=config['bucket_name'],
        )
        for config in shared_config
    }
    return MultiS3RepositoryBackend(primary, shared)


class PsqlS3StorageMigrator(PsqlDosMigrator):

    def get_repository(self) -> S3RepositoryBackend | MultiS3RepositoryBackend:
        return _construct_repository(self.profile.storage_config)
```

Backwards compatible: profiles without `shared_repositories` get exactly today's behavior. The
same helper is reused by `psql_aws_s3.py` / `psql_azure_blob.py` later if desired (their
`get_repository` bodies differ only in the primary constructor).

**Naming constraint:** a shared repository `name` must not contain `/` and must not collide with
the primary bucket semantics; enforce in `shared add` (§6).

## 5. Metadata helpers and attach API (`src/aiida_s3/shared.py`)

The aiida-core serialization format (verified against `aiida/repository/common.py`) is:
directories serialize as `{'o': {<name>: <child>}}` (empty dict `{}` when empty), files as
`{'k': <key>}`. The remap helper only ever touches `'k'` values:

```python
"""Helpers for namespaced shared-object keys and node repository metadata manipulation."""

from __future__ import annotations

import typing as t

from .repository.multi import make_shared_key, split_shared_key

__all__ = ('remap_repository_metadata', 'iter_repository_keys', 'attach_shared_object')


def remap_repository_metadata(metadata: dict, shared_name: str) -> dict:
    """Return a deep copy of node repository metadata with every object key namespaced.

    ``<uuid>`` → ``shared/<shared_name>/<uuid>``. Only the ``'k'`` fields are rewritten; the
    directory structure is preserved as-is.
    """
    if 'k' in metadata:
        parts = split_shared_key(metadata['k'])
        object_key = parts[1] if parts else metadata['k']
        return {'k': make_shared_key(shared_name, object_key)}
    return {'o': {name: remap_repository_metadata(child, shared_name) for name, child in metadata.get('o', {}).items()}}


def iter_repository_keys(metadata: dict, prefix: str = '') -> t.Iterator[tuple[str, str]]:
    """Yield ``(path, key)`` for every file entry in node repository metadata."""
    if 'k' in metadata:
        yield prefix, metadata['k']
    for name, child in metadata.get('o', {}).items():
        yield from iter_repository_keys(child, f'{prefix}/{name}' if prefix else name)


def attach_shared_object(node, path: str, shared_name: str, object_key: str, verify: bool = True) -> None:
    """Reference an existing shared object from ``node`` at ``path`` — no bytes are moved.

    Uses the same metadata-write mechanism as the archive import machinery (the
    ``repository_metadata`` setter on the backend node entity). The node must be stored.
    """
    from aiida.manage import get_manager

    storage = get_manager().get_profile_storage()
    repository = storage.get_repository()
    assert hasattr(repository, 'verify_shared'), 'Storage backend is not a multi-repository backend.'

    if verify:
        from .repository.multi import make_shared_key
        key = make_shared_key(shared_name, object_key)
        if not repository.has_objects([key])[0]:
            raise FileNotFoundError(f'Object `{object_key}` not found in shared repository `{shared_name}`.')

    metadata = dict(node.backend_entity.repository_metadata or {})
    entry: dict = metadata.setdefault('o', {})
    for part in path.strip('/').split('/')[:-1]:
        entry = entry.setdefault(part, {}).setdefault('o', {})
    leaf = path.strip('/').split('/')[-1]
    entry[leaf] = {'k': make_shared_key(shared_name, object_key)}
    node.backend_entity.repository_metadata = metadata
```

## 6. CLI (`src/aiida_s3/cli.py`) — fills the already-declared `aiida-s3` script entry

Commands (click group `cmd_root`, loaded with the current profile like `verdi` commands do):

```
aiida-s3 shared add --name common-data --endpoint-url ... --access-key-id ... \
                    --secret-access-key ... --bucket-name shared-bucket
aiida-s3 shared list                     # name, bucket, reachable?
aiida-s3 shared remove --name common-data
aiida-s3 export-shared [OPTIONS] -o out.aiida-shared
aiida-s3 import-shared out.aiida-shared [--register]
```

### 6.1 `shared add/list/remove` — profile config management

```python
@cmd_root.group('shared')
def cmd_shared():
    """Manage the read-only shared repositories of the current profile."""


@cmd_shared.command('add')
@click.option('-n', '--name', required=True, help='Local name of the shared repository (no slashes).')
@click.option('--endpoint-url', required=True)
@click.option('--access-key-id', required=True)
@click.option('--secret-access-key', required=True)
@click.option('--bucket-name', required=True)
@decorators.with_dbenv()
def cmd_shared_add(name, endpoint_url, access_key_id, secret_access_key, bucket_name):
    """Register a read-only shared repository in the current profile's storage config."""
    if '/' in name or name == '':
        raise click.BadParameter('The name must not contain `/` and must not be empty.')

    from aiida.manage.configuration import get_config
    config = get_config()
    profile = config.get_profile()
    storage_config = profile.storage_config

    entries = storage_config.setdefault('shared_repositories', [])
    if any(entry['name'] == name for entry in entries):
        raise click.ClickException(f'A shared repository named `{name}` is already registered.')

    # Fail fast: probe the bucket before writing the config.
    from aiida_s3.repository.s3 import S3RepositoryBackend
    backend = S3RepositoryBackend(endpoint_url, access_key_id, secret_access_key, bucket_name)
    if not backend.is_initialised:
        raise click.ClickException(f'Bucket `{bucket_name}` does not exist or is not reachable.')

    entries.append({'name': name, 'endpoint_url': endpoint_url, 'access_key_id': access_key_id,
                    'secret_access_key': secret_access_key, 'bucket_name': bucket_name})
    config.update_profile(profile)   # NOTE: verify exact Config API on aiida-core 2.9.3
    config.store()
    click.echo(f'Success: registered shared repository `{name}` -> bucket `{bucket_name}`.')
```

> **Verification item:** the exact `Config`/`Profile` mutation API (`update_profile` vs.
> `add_profile`, whether `storage_config` returns a live reference) must be checked against
> aiida-core 2.9.3 before implementation; this is a three-line concern, but it is the one place
> this plan touches aiida-core configuration internals.

### 6.2 `export-shared` — export the current database as a shared repository

This is the central new command. It produces a **byte-free** archive: entities (nodes, links,
computers, groups) serialized with aiida-core's proven export machinery, plus a manifest of the
shared bucket coordinates — and **no object bytes**.

```
aiida-s3 export-shared --name common-data --all -o common-data.aiida-shared [--verify]
aiida-s3 export-shared --name common-data --group my-pseudos -o common-data.aiida-shared
aiida-s3 export-shared --name common-data --nodes pk1 pk2 ... -o common-data.aiida-shared
```

`--name` is the name under which *importing* profiles will register the source bucket (it does
not need to be registered in the exporting profile). `--verify` (default on) HEADs every
referenced object before writing the archive.

Implementation (`src/aiida_s3/shared_io.py` + command wrapper):

```python
def export_shared(name: str, entities: list, output: pathlib.Path, verify: bool = True) -> None:
    """Export ``entities`` as a shared-repository archive.

    Pipeline:
      1. optional existence check of every repository key of every node (HEAD only)
      2. standard aiida-core archive creation to a temporary file (entities, links, attributes)
      3. post-process the archive zip:
         - remap every node repository_metadata key → ``shared/<name>/<key>``
         - strip every object byte member from the zip
         - inject the shared-repository manifest into metadata.json
      4. write result to ``output``
    """
    from aiida.tools.archive.create import create_archive   # verify import path on 2.9.3

    nodes = [entity for entity in entities if isinstance(entity, orm.Node)]

    # -- 1. fail fast: every referenced object must exist in the primary bucket
    if verify:
        from ..shared import iter_repository_keys
        repository = get_manager().get_profile_storage().get_repository()
        keys = [key for node in nodes for _, key in iter_repository_keys(node.backend_entity.repository_metadata or {})]
        missing = [key for key, exists in zip(keys, repository.has_objects(keys)) if not exists]
        if missing:
            raise ExportError(f'Referenced objects missing from the primary bucket: {missing}')

    # -- 2. byte-complete archive as a temporary artifact (deleted afterwards)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_archive = pathlib.Path(tmpdir) / 'archive.aiida'
        create_archive(entities=entities, filename=tmp_archive)

        # -- 3. post-process
        with zipfile.ZipFile(tmp_archive) as archive:
            data = json.loads(archive.read('data.json'))
            metadata = json.loads(archive.read('metadata.json'))

            # 3a. remap node repository metadata (data.json: 'Node' -> uuid -> 'repository_metadata')
            from ..shared import remap_repository_metadata
            for node_data in data.get('Node', {}).values():
                if node_data.get('repository_metadata'):
                    node_data['repository_metadata'] = remap_repository_metadata(node_data['repository_metadata'], name)

            # 3b. manifest: bucket coordinates only — NEVER credentials
            storage_config = get_manager().get_profile_storage().profile.storage_config
            metadata['shared_repository'] = {
                'name': name,
                'endpoint_url': storage_config['endpoint_url'],
                'bucket_name': storage_config['bucket_name'],
                'key_format': 'uuid4',
                'format': 'aiida-s3-shared/1',
            }

            # 3c. strip object bytes: keep only the entity-reconstruction members
            byte_members = [m for m in archive.namelist() if m not in ('data.json', 'metadata.json')]

            with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as out:
                out.writestr('data.json', json.dumps(data))
                out.writestr('metadata.json', json.dumps(metadata))
                # NOTE: if the 2.9.3 archive format stores anything else needed for entity
                # reconstruction besides data.json/metadata.json, keep those members too.
```

What happened — and what deliberately did **not** happen:

* **No S3 download**: no object was ever read; only `head_object` requests for `--verify`.
* **No S3 upload**: the archive contains zero object bytes, on purpose.
* **No key modification**: objects keep their `uuid4` keys in the source bucket. The
  `shared/<name>/` prefix exists *only inside the archive's `data.json`* — it is routing
  metadata for the importing profile, never an S3 key.
* **No credentials in the archive**: the manifest carries `endpoint_url` + `bucket_name` only.
  Each importing profile supplies its own read-only credentials via `aiida-s3 shared add`.

### 6.3 `import-shared` — reference the objects in another profile/database

```
aiida-s3 import-shared common-data.aiida-shared            # shared repo already registered
aiida-s3 import-shared common-data.aiida-shared --register  # register from the manifest, then import
```

```python
def import_shared(archive_path: pathlib.Path, register: bool = False) -> None:
    """Import a shared-repository archive into the current profile — no byte transfer."""
    with zipfile.ZipFile(archive_path) as archive:
        data = json.loads(archive.read('data.json'))
        metadata = json.loads(archive.read('metadata.json'))

    manifest = metadata.get('shared_repository')
    if not manifest or manifest['format'] != 'aiida-s3-shared/1':
        raise ImportError('Not a shared-repository archive; use `verdi archive import` instead.')
    name = manifest['name']

    # -- 1. ensure the shared repository is registered (and reachable) in this profile
    if register:
        register_shared_repository(  # same code path as `aiida-s3 shared add`
            name=name,
            endpoint_url=manifest['endpoint_url'],
            # credentials must come from the environment/cli: they are NOT in the archive
            access_key_id=click.prompt(f'Access key ID for shared repository `{name}`', hide_input=True),
            secret_access_key=click.prompt('Secret access key', hide_input=True),
            bucket_name=manifest['bucket_name'],
        )
    repository = get_manager().get_profile_storage().get_repository()
    if not hasattr(repository, 'verify_shared') or not repository.verify_shared().get(name, False):
        raise ImportError(f'Shared repository `{name}` not registered/reachable in this profile.')

    # -- 2. rehydrate: rebuild a standard archive with placeholder bytes.
    #    aiida-core's import validates that every repository key referenced by node metadata
    #    is present in the archive, and imports objects by explicit key. Placeholders satisfy
    #    the validation; the actual put is a no-op in MultiS3RepositoryBackend (existence check
    #    only — the placeholder bytes are discarded, nothing reaches S3).
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_archive = pathlib.Path(tmpdir) / 'rehydrated.aiida'
        with zipfile.ZipFile(tmp_archive, 'w') as out:
            out.writestr('data.json', json.dumps(data))
            out.writestr('metadata.json', json.dumps(metadata))
            from ..shared import iter_repository_keys
            for node_data in data.get('Node', {}).values():
                rm = node_data.get('repository_metadata')
                if not rm:
                    continue
                for path_, key in iter_repository_keys(rm):
                    out.writestr(f'repo/{key}', b'')   # placeholder; layout per 2.9.3 archive format — verify

        from aiida.tools.archive import import_archive   # verify import path on 2.9.3
        import_archive(tmp_archive)

    # -- 3. post-verify: every shared key must resolve in the shared bucket
    from ..shared import iter_repository_keys
    keys = [key for nd in data.get('Node', {}).values() if nd.get('repository_metadata')
            for _, key in iter_repository_keys(nd['repository_metadata'])
            if key.startswith('shared/')]
    if not all(repository.has_objects(keys)):
        raise ImportError('Post-import verification failed: some shared objects are not readable.')
```

What happened — and what deliberately did **not** happen:

* **No S3 download**: node contents are never fetched during import.
* **No S3 upload**: `MultiS3RepositoryBackend._put_object_from_filelike` discards the
  placeholder handle after verifying the object exists in the shared bucket (`HEAD`).
* **No key modification**: the imported nodes' `repository_metadata` contains the namespaced
  keys exactly as exported; the shared bucket is untouched.
* The bytes in the rehydrated archive are **empty placeholders** — a purely local, in-memory
  artifact to satisfy aiida-core's archive validation, never stored anywhere persistent and
  never uploaded.

## 7. Caveats, risks, and operational rules

**Correctness risks to verify during implementation (all contained to `shared_io.py`):**

1. **Archive member layout.** The exact zip layout of aiida-core 2.9.3 export archives
   (`data.json`/`metadata.json` member names, the `repo/<key>` object path convention, whether
   anything else is needed for reconstruction) is an internal format. Pin the tests to
   aiida-core 2.9.3 and adapt `shared_io.py` if it differs. This is the price of keeping the
   import/export logic out of aiida-core — and it is one file to fix, not a fork of the core.
2. **Key reuse on import.** The whole flow relies on aiida-core ≥ 2.7 behavior where archive
   import calls `_put_object_from_filelike(handle, key=…)` with the archived key when
   `key_format` matches (present in 2.9.3, per the `archive_format`/`key` upstream change
   mirrored by commit `4870c06`). On versions where import regenerates keys, namespaced keys
   would be replaced by fresh UUID4s pointing at nothing — the post-import verification in
   step 3 catches this loudly. Recommendation: declare the multi-repo feature as requiring
   `aiida-core>=2.9` in the docs, even though the package floor stays `~=2.6`.
3. **`Config.update_profile` API.** See the note in §6.1.
4. **`aiida.tools.archive.create/import` import paths** must be checked against 2.9.3
   (`create_archive` signature, `import_archive` conflict-handling defaults).

**Operational rules (document in README alongside the feature):**

* Read-only enforcement is **IAM-first**: give importing profiles `s3:GetObject` (and
  `s3:ListBucket` if you want bucket-existence checks) on the shared bucket only. The
  `PermissionError` in `delete_objects` is UX, not security.
* **Garbage collection of the shared bucket is admin-owned.** No profile can delete shared
  objects, so no profile can decide when they are unreferenced. Deleting from the shared bucket
  while any profile still references a key leaves dangling references that surface as
  `FileNotFoundError` on access (and as failures of the import post-verification). If needed, a
  future `aiida-s3 shared gc --dry-run` can walk the registered profiles' `repository_metadata`
  to compute the reference set — out of scope here.
* **Vanilla archives do not preserve shared-ness.** `verdi archive export` on a profile that
  references shared objects embeds the object *bytes* (it gathers content streams). Reimporting
  into another profile materializes them as plain local objects. That is by design: use
  `aiida-s3 export-shared` / `import-shared` to preserve references, `verdi archive` for
  self-contained copies. Flag this loudly in the README table of the two workflows.
* **Cross-S3-server targets need a one-time byte copy, out of band.** `import-shared` assumes
  the target profile can reach the shared bucket (same S3 server, VPC peering, or public
  read-only bucket). For an air-gapped target, sync the bucket once with `aws s3 sync` (or
  bucket replication) and point the manifest's `endpoint_url` at the mirror; the import flow is
  otherwise identical.
* **Name stability.** The shared repository `name` is baked into imported `repository_metadata`.
  Renaming a registration later breaks routing (the `KeyError` in `_route` is loud). Treat names
  as immutable identifiers.
* **Concurrent imports are safe** — aiida-core's import deduplicates entities by UUID, and the
  no-op put is idempotent (existence check).

## 8. Test plan

* `tests/repository/test_multi.py` (moto, no credentials):
  routing of `open`/`has_objects` by namespace; prefix stripped (assert boto3 calls see bare
  keys via stubber); no-op put with explicit shared key (existing vs missing object);
  `PermissionError` on shared-key delete; `erase`/`list_objects`/`maintain` never touch shared
  buckets; primary-only behaviour when no shared config (regression: plain profile identical to
  today).
* `tests/test_shared.py`: `remap_repository_metadata` roundtrip on nested metadata;
  `attach_shared_object` writes correct entry and verifies existence.
* `tests/cli/test_shared_io.py` (two pgtest profiles, moto):
  1. profile A: create nodes with files, `export-shared --name common --all`;
  2. assert the archive contains no object bytes and a correct manifest;
  3. profile B: `shared add` (pointing at A's bucket), `import-shared`;
  4. assert B's bucket received **zero** objects, B's nodes' metadata contain namespaced keys,
     and `node.get_object_content(path)` streams from the shared bucket;
  5. `--verify` failure path when a key is missing.

## 9. Implementation order

1. `multi.py` + `tests/repository/test_multi.py` (fully independent of everything else).
2. `get_repository` change in `psql_s3.py` (+ config helper) — opt-in, zero risk to existing
   profiles.
3. `shared.py` helpers + tests.
4. `cli.py` `shared add/list/remove` (unblocks manual experimentation).
5. `shared_io.py` export → import roundtrip + tests (items 1–3 of §7 verified here).
6. README section: configuration, the two workflows (shared vs vanilla archives), IAM guidance.
