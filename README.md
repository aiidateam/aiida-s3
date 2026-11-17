# `aiida-s3`

AiiDA plugin that provides various storage backends that allow using cloud data storage services, such as AWS S3 and Azure Blob Storage.

Currently, the following storage backends are available:

* `s3.psql_s3`: Database provided by PostgreSQL and file repository provided by any service implementing the S3 protocol, for example [minIO](https://min.io).
* `s3.psql_aws_s3`: Database provided by PostgreSQL and file repository provided by [AWS S3](https://aws.amazon.com/s3/).
* `s3.psql_azure_blob`: Database provided by PostgreSQL and file repository provided by [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/).

In addition, any profile using the `s3.psql_s3` backend can reference **read-only shared repositories**:
S3 buckets whose objects are stored once and referenced by many profiles, without any data duplication. See the
[Shared repositories](#shared-repositories) section and, for the motivation, [Why shared data
matters](#why-shared-data-matters-multi-tenant-platforms).

## Installation

The recommended method of installation is through the [`pip`](https://pip.pypa.io/en/stable/) package installer for Python:

    pip install aiida-s3

## Setup

To use one of the storage backends provided by `aiida-s3` with AiiDA, you need to create a profile for it.
This can be done through AiiDA's CLI `verdi`:

1.  List the available storage backends:
    ```bash
    verdi profile setup --help
    ```

1.  Create a profile using one of the available storage backends by passing it as an argument to `verdi profile setup`, for example:
    ```bash
    verdi profile setup s3.psql_s3
    ```
    The command will prompt for the information required to setup the storage backend.
    After all information is entered, the storage backend is initialized, such as creating the database schema and creating file containers.

1.  The profile is now ready to be used with AiiDA.
    Optionally, to test that everything is working as intended, launch a test calculation:
    ```bash
    verdi -p profile-name devel launch-add
    ```

## Why shared data matters: multi-tenant platforms

AiiDA's storage model is one profile = one database + one object repository. That is exactly right for a
single research group, but it becomes expensive on **shared online platforms** (SaaS, HPC portals, university
clusters) that host many isolated accounts, each with its own AiiDA profile, its own bucket, and therefore its
own copy of every input file. The heaviest and most duplicated content is well-known common data: the
[SSSP](https://www.materialscloud.org/work/tools/sssp) and [PseudoDojo](https://www.pseudo-dojo.org/)
pseudopotential families, structure prototypes, common codes and parameters.

The numbers from a real deployment that installs the full pseudo set (SSSP 1.3 + PseudoDojo) per account:

| | per account | platform with 1,000 accounts |
|---|---|---|
| object storage without sharing | ~700 MB of pseudo files | ~700 GB |
| object storage with a shared bucket | 0 bytes (key references only; ~10 KB manifest) | ~700 MB total, **~99.9% saved** |

Only the nodes' database rows are per-account; the file bytes exist once, in the shared bucket. The same
one-time cost applies regardless of whether the platform has 100 or 100,000 accounts (7 TB saved at 10k
accounts). Sharing also removes the per-account *time* cost: installing the families takes a 700 MB download
plus UPF parsing per account (minutes to tens of minutes), while importing them as shared references is a
metadata-only operation (one DB insert per node, zero content transfer) that takes seconds.

### A note for aiida-core developers

Everything in this plugin works through public repository/storage interfaces, but two core behaviors make
shared references harder than they need to be on stock `aiida-core`:

1. **Archive imports regenerate object keys.** On stock core, an imported node always gets fresh keys, so
   the imported bytes must be materialized in the importing profile's own bucket, even when they are
   identical to the source's. The shared *archive* path of this plugin therefore requires a patched
   `aiida-core` (2.9.3 plus the UUID4-key archive patches: the `archive_format`/`maintain_keys` hooks).
   Upstreaming those hooks would make key-preserving imports a first-class capability.
2. **Node construction re-derives content metadata.** Importing a `Data` node normally requires the file
   content (the node class parses it, and e.g. `aiida-pseudo` re-validates the md5 by re-reading the whole
   file). For a trusted, already-parsed export manifest this is pure waste. The shared *collection* path of
   this plugin demonstrates the alternative on stock core: nodes are created empty, their repository
   metadata is pre-armed with the shared keys, and the attributes (including the parsed content metadata)
   come verbatim from the manifest. A "trusted metadata import" mode in core would generalize this.

The takeaway we would like to share: a `MultiRepositoryBackend` that routes some key namespaces to
read-only external buckets is a small, non-invasive extension of the storage abstraction, and it changes
the economics of running AiiDA at platform scale.

## Shared repositories

Any profile using the `s3.psql_s3` storage backend can reference any number of **read-only shared
repositories**: S3 buckets that many separated profiles can reference. This is useful for data that is
needed everywhere, such as pseudopotentials or other common data files. The central invariant:

> No S3 upload, download, or key modification ever happens against a shared bucket.
> Shared objects are referenced by key; the bytes live only in the shared bucket, once.

When the profile's storage configuration carries a `shared_repositories` entry, the repository of that
profile is a `MultiS3RepositoryBackend`: the profile's own read-write bucket (primary) plus the shared
read-only buckets, whose objects are namespaced as `shared/<name>/<object-key>` and routed transparently
to the right concrete backend. Profiles created without `shared_repositories` keep the plain
`S3RepositoryBackend` exactly as before - the feature is purely opt-in.

### Registering a shared repository

A shared repository is registered in the profile's storage configuration under the optional
`shared_repositories` entry, managed through the `aiida-s3` CLI:

```bash
aiida-s3 shared add --name common-data --endpoint-url http://minio.internal:9000 \
                    --access-key-id read-only-key --secret-access-key ... \
                    --bucket-name shared-bucket
aiida-s3 shared list
aiida-s3 shared remove --name common-data
```

The `name` is baked into the object keys of imported nodes (`shared/<name>/<object-key>`) and must be
treated as an immutable identifier: renaming a registration breaks the routing. Read-only enforcement is
IAM-first: give importing profiles credentials with `s3:GetObject` on the shared bucket only.

#### Deleting nodes that reference shared objects

`MultiS3RepositoryBackend.delete_objects` is **soft-delete by default** (`soft_delete_shared=True`):
shared-namespaced keys are filtered out and their S3 objects are preserved - only the profile's own database
records are removed, never the referenced objects, which other profiles may still use. With
`soft_delete_shared=False` any shared key in the deletion list raises `PermissionError` instead. Both are
UX, not security: the shared bucket must be read-only at the IAM level.

#### The S3 region (operational note)

The profile storage configuration may carry an optional `region_name`; it is passed to the boto3 clients.
Leave it out (or set it to `None`) when the presigned URLs generated by these clients are served through a
reverse proxy that rewrites the host: with a region, boto3 signs SigV4 (`X-Amz-SignedHeaders=host`) and the
URL breaks when the host changes in transit; without a region, boto3 falls back to SigV2, which does not
sign the host. Both configurations work for normal requests; only presigned-URL hosting is affected.

### Sharing data with other profiles

Two mechanisms are provided, both zero-byte on the wire:

**Shared collections** (recommended; provenance-free JSON manifests, no `aiida-core` patches needed):

```bash
# on the exporting profile (e.g. one pseudo family into a manifest)
aiida-s3 collection export --name common-data --group "SSSP/1.3/PBE/efficiency" \
                          --output sssp-1.3-pbe-efficiency.json

# on the importing profile (--register prompts for the read-only credentials)
aiida-s3 collection import sssp-1.3-pbe-efficiency.json --register
```

The manifest contains the nodes' metadata (attributes, extras, labels - including everything the node class
had parsed, such as UPF metadata, and the groups' extras such as SSSP/PseudoDojo cutoffs) and, per file,
the key of the object in the shared bucket: no bytes. The import creates each node empty with its repository
metadata pre-armed at the shared keys, one database insert per node and **zero S3 content requests**; the
pseudo md5 re-validation on store (which would re-download the file) is skipped by storing through the
parent class implementation, so a full SSSP+PseudoDojo set (~2,500 nodes) imports in seconds. Collections
are also usable as a library (`aiida_s3.shared_collection.export_shared_collection` /
`import_shared_collection`) from services that manage several profiles explicitly, without any globally
loaded AiiDA profile.

**Shared archives** (`aiida-core` archive-based; requires the patched `aiida-core` - see the note for
developers above):

```bash
aiida-s3 export-shared --name common-data --all -o common-data.aiida-shared
aiida-s3 import-shared common-data.aiida-shared --register
```

The archive contains the entities (nodes, groups, links) and a manifest with the bucket coordinates, but
**no object bytes**; the import preserves the object keys so the files reference the shared bucket by key.

Use `verdi archive export`/`import` for self-contained copies that embed the object bytes. Note that a
vanilla archive exported from a profile that references shared objects materializes those objects as plain
local objects in the importing profile: use `aiida-s3` shared collections or archives to preserve the
references instead. Exporting nodes that themselves reference shared objects is refused: the bytes would
not be reachable through the exported bucket.

### Caveats

* Garbage collection of the shared bucket is admin-owned: no profile can delete shared objects, so no
  profile can decide when they are unreferenced. Deleting from the shared bucket while profiles still
  reference a key leaves dangling references that surface as `FileNotFoundError` on access.
* The importing profile must be able to reach the shared bucket (same S3 server, VPC peering, or a public
  read-only bucket). For an air-gapped target, sync the bucket once out of band (e.g. `aws s3 sync` or
  bucket replication) and point the shared repository registration at the mirror.
* Shared collections import each node with a **new UUID** and no provenance (no links): they are for
  distributing common data, not for migrating workflows. Use shared archives or vanilla archives when
  provenance matters.

## Retrocompatibility

* The three storage backends (`s3.psql_s3`, `s3.psql_aws_s3`, `s3.psql_azure_blob`) and the profiles using
  them are **unchanged**: existing profiles, keys and buckets work exactly as before the shared-repository
  feature existed.
* `shared_repositories` is optional: a profile without it gets the plain `S3RepositoryBackend`, with
  identical behavior and storage layout. Adding shared repositories to an existing profile later only
  extends what its keys can reference; existing objects are untouched.
* `region_name` in the storage configuration is optional (it always was): old configurations that carry a
  region keep working; only the presigned-URL hosting consideration above applies.
* Shared collections need only stock `aiida-core` (2.x) plus the node plugins of the exported types (e.g.
  `aiida-pseudo`; both the old `PseudoPotential` and the renamed `PseudoPotentialData` classes are
  supported). The md5 re-validation bypass applies exclusively to pseudo nodes during a collection import;
  every other node class stores through its normal `store()`.
* Shared archives require the patched `aiida-core` (2.9.3 + UUID4-key archive patches): on stock core, the
  import regenerates the object keys and the import fails the post-verification loudly. Plain
  `verdi archive import`/`export` is unaffected on any core.
* The soft-delete behavior of `MultiS3RepositoryBackend.delete_objects` is the default but configurable
  (`soft_delete_shared`), so integrations that want the strict `PermissionError` can opt back in.

## Testing

The unit tests are implemented and run with [`pytest`](https://docs.pytest.org/).
To run them, install the package with the `tests` extra dependencies:

    pip install aiida-s3[tests]

The plugin provides interfaces to various services that require credentials, such as AWS S3 and Azure Blob Storage.
To run the test suite, one has to provide these credentials or the services have to be mocked.
Instructions for each service that is supported are provided below.

### S3

The base S3 implementation is interfaced with through the [`boto3`](https://pypi.org/project/boto3/) Python SDK.
The [`moto`](https://pypi.org/project/moto/) library allows to mock this interface.
This makes it possible to run the test suite without any credentials.
To run the tests, simply execute `pytest`:

    pytest

By default, the interactions with S3 are mocked through `moto` and no actual credentials are required.
To run the tests against an actual S3 server, the endpoint URL and credentials need to be specified through environment variables:

    export AIIDA_S3_MOCK_S3=False
    export AIIDA_S3_ENDPOINT_URL='http://localhost:9000'
    export AIIDA_S3_BUCKET_NAME='some-bucket'
    export AIIDA_S3_ACCESS_KEY_ID='access-key'
    export AIIDA_S3_SECRET_ACCESS_KEY='secret-access-key'
    pytest

One example of an open source implementation of a S3-compatible object store is [minIO](https://min.io/).
An instance can easily be created locally using Docker and `docker-compose`.
Simply write the following to `docker-compose.yml`:

    version: '2'

    services:
      minio:
        container_name: Minio
        command: server /data --console-address ":9001"
        environment:
          - MINIO_ROOT_USER=admin
          - MINIO_ROOT_PASSWORD=supersecret
        image: quay.io/minio/minio:latest
        ports:
          - '9000:9000'
          - '9001:9001'
        volumes:
          - /tmp/minio:/data
        restart: unless-stopped

and then launch the container with:

    docker-compose up -d

The tests can then be run against the server using environment variables as described above.


### AWS S3

The [AWS S3](https://aws.amazon.com/s3/) service is interfaced with through the [`boto3`](https://pypi.org/project/boto3/) Python SDK.
The [`moto`](https://pypi.org/project/moto/) library allows to mock this interface.
This makes it possible to run the test suite without any credentials.
To run the tests, simply execute `pytest`:

    pytest

By default, the interactions with AWS S3 are mocked through `moto` and no actual credentials are required.
To run the tests against an actual AWS S3 container, the credentials need to be specified through environment variables:

    export AIIDA_S3_MOCK_AWS_S3=False
    export AIIDA_S3_AWS_BUCKET_NAME='some-bucket'
    export AIIDA_S3_AWS_ACCESS_KEY_ID='access-key'
    export AIIDA_S3_AWS_SECRET_ACCESS_KEY='secret-access-key'
    pytest


### Azure Blob Storage

The [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/) is communicated with through the [`azure-blob-storage`](https://pypi.org/project/azure-storage-blob/) Python SDK.
Currently, there is no good way to mock the clients of this library.
Therefore, when the tests are run without credentials, and so the Azure Blob Storage client needs to be mocked, the tests are skipped.
To run the tests against an actual AWS S3 container, the credentials need to be specified through environment variables:

    export AIIDA_S3_MOCK_AZURE_BLOB=False
    export AIIDA_S3_AZURE_BLOB_CONTAINER_NAME='some-container'
    export AIIDA_S3_AZURE_BLOB_CONNECTION_STRING='DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net'
    pytest

The specified container does not have to exist yet, it will be created automatically.
The connection string can be obtained through the Azure portal.
