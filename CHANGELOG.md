# CHANGELOG

## Unreleased

### Features
- Add `MultiS3RepositoryBackend`: a repository spanning one read-write primary and any number of read-only shared
  S3 repositories, addressed by namespaced object keys (`shared/<name>/<object-key>`)
- `s3.psql_s3`: support the optional `shared_repositories` entry in the profile storage configuration
- Add the `aiida-s3` CLI with the `shared add/list/remove` commands to manage the shared repositories of the current
  profile, and the `export-shared`/`import-shared` commands to share data between profiles by reference, without
  transferring any object bytes
- Add `aiida_s3.shared` with helpers to remap node repository metadata and to attach existing shared objects to
  stored nodes

### Bug fixes
- `S3RepositoryBackend`: allow `endpoint_url` to be omitted in the storage configuration, in which case the default
  AWS endpoint is used

## `v0.2.0` - 2023-02-27

### Improvements
- Add support for any service that implements the S3 protocol through the `S3RepositoryBackend` base class [[#13]](https://github.com/sphuber/aiida-s3/pull/13)
- `S3RepositoryBackend`: Remove double request in `list_objects` [[#17]](https://github.com/sphuber/aiida-s3/pull/17)
- `S3RepositoryBackend`: Do not rely on `list_objects` for `open` [[#14]](https://github.com/sphuber/aiida-s3/pull/14)

### Bug fixes
- `S3RepositoryBackend`: Fix bug in `list_objects` being incomplete returning only part of all objects for repositories exceeding 1000 objects [[#18]](https://github.com/sphuber/aiida-s3/pull/18)

### DevOps
- DevOps: Add continuous-deployment workflow [[a7eb601b]](https://github.com/sphuber/aiida-s3/commit/a7eb601b50367954efe8cbd9e059e55adbfe4192)
- Dependencies: Update pre-commit requirement `isort==5.12.0` [[#12]](https://github.com/sphuber/aiida-s3/pull/12)
- Package: Update development status to beta [[#19]](https://github.com/sphuber/aiida-s3/pull/19)
- Package: Update the package URLs in `pyproject.toml` [[#19]](https://github.com/sphuber/aiida-s3/pull/19)


## `v0.1.0` - 2022-12-04

First release, with support for AWS S3 and Azure Blob Storage.
