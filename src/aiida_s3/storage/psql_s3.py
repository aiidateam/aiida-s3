"""Implementation of :class:`aiida.orm.implementation.storage_backend.StorageBackend` using PostgreSQL + S3."""

from __future__ import annotations

import typing as t

from aiida.common.pydantic import MetadataField
from aiida.storage.psql_dos.backend import PsqlDosBackend
from aiida.storage.psql_dos.migrator import PsqlDosMigrator
from pydantic import ConfigDict

from ..repository.multi import MultiS3RepositoryBackend
from ..repository.s3 import S3RepositoryBackend


def construct_repository(storage_config: dict[str, t.Any]) -> S3RepositoryBackend | MultiS3RepositoryBackend:
    """Construct the file repository backend from the profile storage configuration.

    If the optional ``shared_repositories`` entry is defined in the storage configuration, the returned backend is
    a :class:`aiida_s3.repository.multi.MultiS3RepositoryBackend` that routes ``shared/<name>/<key>`` object keys to
    the shared repository registered under ``<name>``. Otherwise, a plain
    :class:`aiida_s3.repository.s3.S3RepositoryBackend` is returned.

    The optional ``soft_delete_shared`` entry (default: ``True``) controls whether deletion of shared-namespaced
    keys is silently skipped (preserving the S3 objects in the shared buckets) or raises ``PermissionError``.

    :param storage_config: the storage configuration of the profile.
    :return: the file repository backend instance.
    """
    primary = S3RepositoryBackend(
        endpoint_url=storage_config['endpoint_url'],
        access_key_id=storage_config['access_key_id'],
        secret_access_key=storage_config['secret_access_key'],
        bucket_name=storage_config['bucket_name'],
        region_name=storage_config.get('region_name'),
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
            region_name=config.get('region_name', storage_config.get('region_name')),
        )
        for config in shared_config
    }
    return MultiS3RepositoryBackend(
        primary,
        shared,
        soft_delete_shared=storage_config.get('soft_delete_shared', True),
    )


class PsqlS3StorageMigrator(PsqlDosMigrator):
    """Subclass :class:`aiida.storage.psql_dos.migrator.PsqlDosMigrator` to customize the repository implementation."""

    def get_repository_uuid(self) -> str:
        """Return the UUID of the repository.

        :returns: The UUID of the repository of the configured profile.
        """
        return self.get_repository().uuid

    def reset_repository(self) -> None:
        """Reset the repository deleting the bucket and all its contents."""
        repository = self.get_repository()
        if repository.is_initialised:
            repository.erase()

    def initialise_repository(self) -> None:
        """Initialise the repository."""
        repository = self.get_repository()
        repository.initialise()

    @property
    def is_repository_initialised(self) -> bool:
        """Return whether the repository is initialised.

        :returns: Boolean, ``True`` if the repository is initalised, ``False`` otherwise.
        """
        return self.get_repository().is_initialised

    def get_repository(self) -> S3RepositoryBackend | MultiS3RepositoryBackend:
        """Return the file repository backend instance.

        :returns: The repository of the configured profile.
        """
        return construct_repository(self.profile.storage_config)


class PsqlS3Storage(PsqlDosBackend):
    """Storage backend using PostgresSQL and S3 object store."""

    migrator = PsqlS3StorageMigrator

    _repository: S3RepositoryBackend | MultiS3RepositoryBackend | None = None

    class CliModel(PsqlDosBackend.CliModel):
        """Model describing required information to configure an instance of the storage.

        Extra fields are allowed, such that optional plugin-specific entries like ``shared_repositories`` (managed
        through the ``aiida-s3 shared add`` command and not part of the setup prompts) survive profile creation.
        """

        model_config: t.ClassVar[ConfigDict] = ConfigDict(
            **PsqlDosBackend.CliModel.model_config,
            extra='allow',
        )

        endpoint_url: str | None = MetadataField(
            title='Server endpoint URL',
            description=(
                'The endpoint URL of the server to connect to. If not specified, the default AWS endpoint is used.'
            ),
        )
        access_key_id: str = MetadataField(
            title='Access key ID',
            description='The access key ID.',
        )
        secret_access_key: str = MetadataField(
            title='Secret access key',
            description='The secret access key.',
        )
        bucket_name: str = MetadataField(
            title='Bucket name',
            description='The name of the S3 bucket to use.',
        )
        repository_uri: t.ClassVar[None]  # type: ignore[assignment,misc]

    def get_repository(self) -> S3RepositoryBackend | MultiS3RepositoryBackend:  # type: ignore[override]
        """Return the file repository backend instance.

        :returns: The repository of the configured profile.
        """
        if self._repository is None:
            self._repository = self.migrator(self.profile).get_repository()
        return self._repository
