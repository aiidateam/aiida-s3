"""Command line interface for the ``aiida-s3`` plugin."""

from __future__ import annotations

import json
import pathlib
import typing as t
import zipfile

import click
from aiida.cmdline.utils import decorators


@click.group('aiida-s3')
def cmd_root() -> None:
    """Commands for the ``aiida-s3`` storage backends and shared repositories."""


@cmd_root.group('shared')
def cmd_shared() -> None:
    """Manage the read-only shared repositories of the current profile."""


def _raise_click_exception(exception: Exception) -> None:
    """Re-raise the exception as a ``click.ClickException`` with the same message."""
    raise click.ClickException(str(exception)) from exception


@cmd_shared.command('add')
@click.option('-n', '--name', required=True, help='Local name of the shared repository (no slashes).')
@click.option('--endpoint-url', required=True, help='Endpoint URL of the server hosting the shared bucket.')
@click.option('--access-key-id', required=True, help='Access key ID with read-only access to the shared bucket.')
@click.option('--secret-access-key', required=True, help='Secret access key with read-only access to the bucket.')
@click.option('--bucket-name', required=True, help='Name of the shared bucket.')
@decorators.with_dbenv()
def cmd_shared_add(name: str, endpoint_url: str, access_key_id: str, secret_access_key: str, bucket_name: str) -> None:
    """Register a read-only shared repository in the current profile's storage configuration."""
    from aiida.manage.configuration import get_profile

    from aiida_s3.shared import add_shared_repository

    profile = get_profile()
    assert profile is not None, 'No profile has been loaded.'

    if not profile.storage_backend.startswith('s3.'):
        raise click.ClickException(
            f'The current profile uses storage backend `{profile.storage_backend}`, which does not support shared '
            'repositories.'
        )

    try:
        add_shared_repository(name, endpoint_url, access_key_id, secret_access_key, bucket_name)
    except (ValueError, FileNotFoundError) as exception:
        _raise_click_exception(exception)

    click.echo(f'Success: registered shared repository `{name}` -> bucket `{bucket_name}`.')


@cmd_shared.command('list')
@decorators.with_dbenv()
def cmd_shared_list() -> None:
    """List the shared repositories registered in the current profile."""
    from aiida.manage import get_manager

    from aiida_s3.shared import get_shared_repositories

    entries = get_shared_repositories()

    if not entries:
        click.echo('No shared repositories registered.')
        return

    repository = get_manager().get_profile_storage().get_repository()
    reachable = repository.verify_shared() if hasattr(repository, 'verify_shared') else {}

    for entry in entries:
        click.echo(
            f'{entry["name"]}: bucket `{entry["bucket_name"]}` on `{entry["endpoint_url"]}`'
            f'{"" if reachable.get(entry["name"], False) else " [unreachable]"}'
        )


@cmd_shared.command('remove')
@click.option('-n', '--name', required=True, help='Local name of the shared repository to remove.')
@decorators.with_dbenv()
def cmd_shared_remove(name: str) -> None:
    """Remove a shared repository from the current profile's storage configuration.

    Note that the name is baked into the object keys of imported nodes: removing the registration breaks the
    routing to all nodes that reference its objects.
    """
    from aiida_s3.shared import remove_shared_repository

    try:
        remove_shared_repository(name)
    except ValueError as exception:
        _raise_click_exception(exception)

    click.echo(f'Success: removed shared repository `{name}`.')


@cmd_root.command('export-shared')
@click.option('-n', '--name', required=True, help='Name under which importing profiles register the shared bucket.')
@click.option('-o', '--output', required=True, type=click.Path(path_type=pathlib.Path), help='Output archive path.')
@click.option('-a', '--all', 'all_', is_flag=True, help='Export all nodes and groups of the current profile.')
@click.option('-N', '--nodes', multiple=True, type=str, help='PKs of the nodes to export.')
@click.option('-G', '--groups', multiple=True, type=str, help='Labels of the groups to export.')
@click.option(
    '--verify/--no-verify',
    default=True,
    show_default=True,
    help='Verify that every referenced object exists in the primary bucket before exporting.',
)
@decorators.with_dbenv()
def cmd_export_shared(  # noqa: PLR0913
    name: str, output: pathlib.Path, all_: bool, nodes: tuple[str, ...], groups: tuple[str, ...], verify: bool
) -> None:
    """Export entities as a shared-repository archive, containing no object bytes.

    Importing profiles will reference the objects in the primary bucket of this profile read-only, by key.
    """
    from aiida import orm

    from aiida_s3.shared_io import export_shared

    entities: list[t.Any] = []

    if all_:
        entities.extend(orm.QueryBuilder().append(orm.Node).all(flat=True))
        entities.extend(orm.Group.objects.all())

    for pk in nodes:
        entities.append(orm.load_node(pk=int(pk)))

    for label in groups:
        entities.append(orm.load_group(label=label))

    if not entities:
        raise click.UsageError('Nothing to export: specify `--all`, `--nodes` and/or `--groups`.')

    try:
        export_shared(name, entities, output, verify=verify)
    except (ValueError, FileNotFoundError) as exception:
        _raise_click_exception(exception)

    click.echo(f'Success: shared repository archive written to `{output}`.')


@cmd_root.command('import-shared')
@click.argument('archive', type=click.Path(exists=True, path_type=pathlib.Path))
@click.option(
    '--register',
    is_flag=True,
    help='Register the shared repository from the archive manifest before importing, prompting for credentials.',
)
@decorators.with_dbenv()
def cmd_import_shared(archive: pathlib.Path, register: bool) -> None:
    """Import a shared-repository archive into the current profile, transferring no object bytes.

    The nodes are imported such that they reference the objects in the shared bucket by key.
    """
    from aiida_s3.shared_io import METADATA_NAME, import_shared

    credentials: dict[str, str] | None = None

    if register:
        with zipfile.ZipFile(archive, 'r') as handle:
            manifest = json.loads(handle.read(METADATA_NAME)).get('shared_repository', {})
        name = manifest.get('name', 'the shared repository')
        credentials = {
            'access_key_id': click.prompt(f'Access key ID for shared repository `{name}`', hide_input=True),
            'secret_access_key': click.prompt('Secret access key', hide_input=True),
        }

    try:
        import_shared(archive, credentials)
    except (ValueError, FileNotFoundError) as exception:
        _raise_click_exception(exception)

    click.echo(f'Success: imported shared repository archive `{archive}`.')


@cmd_root.group('collection')
def cmd_collection() -> None:
    """Export/import Data nodes and Groups as shared collections (no provenance)."""


@cmd_collection.command('export')
@click.option('-n', '--name', required=True, help='Name under which importing profiles register the shared bucket.')
@click.option('-o', '--output', required=True, type=click.Path(path_type=pathlib.Path), help='Output manifest path (JSON).')
@click.option('-d', '--data', 'data_pks', multiple=True, type=str, help='PKs of Data nodes to export (can be repeated).')
@click.option('-D', '--data-uuid', 'data_uuids', multiple=True, type=str, help='UUIDs of Data nodes to export (can be repeated).')
@click.option('-G', '--group', 'group_labels', multiple=True, type=str, help='Labels of Groups to export (can be repeated).')
@click.option(
    '--all-data',
    is_flag=True,
    help='Export all Data nodes of the current profile.',
)
@click.option(
    '--all-groups',
    is_flag=True,
    help='Export all Groups of the current profile.',
)
@click.option(
    '--verify/--no-verify',
    default=True,
    show_default=True,
    help='Verify that every referenced object exists in the primary bucket before exporting.',
)
@decorators.with_dbenv()
def cmd_collection_export(  # noqa: PLR0913
    name: str,
    output: pathlib.Path,
    data_pks: tuple[str, ...],
    data_uuids: tuple[str, ...],
    group_labels: tuple[str, ...],
    all_data: bool,
    all_groups: bool,
    verify: bool,
) -> None:
    """Export Data nodes and Groups as a shared collection manifest (JSON).

    The manifest contains all metadata needed to recreate the nodes in another
    profile, with repository files referenced by key in the source S3 bucket.
    No provenance is preserved - this is for sharing reference data like
    pseudopotential families.
    """
    from aiida import orm

    from aiida_s3.shared_collection import export_shared_collection

    data_nodes: list[t.Any] = []
    groups: list[t.Any] = []

    # Collect data nodes
    if all_data:
        data_nodes.extend(orm.QueryBuilder().append(orm.Data).all(flat=True))

    for pk in data_pks:
        node = orm.load_node(pk=int(pk))
        if not isinstance(node, orm.Data):
            raise click.UsageError(f'Node {pk} is not a Data node: {type(node)}')
        data_nodes.append(node)

    for uuid in data_uuids:
        node = orm.load_node(uuid=uuid)
        if not isinstance(node, orm.Data):
            raise click.UsageError(f'Node {uuid} is not a Data node: {type(node)}')
        data_nodes.append(node)

    # Collect groups
    if all_groups:
        groups.extend(orm.Group.objects.all())

    for label in group_labels:
        group = orm.load_group(label=label)
        groups.append(group)

    # If groups are specified, automatically include their Data members
    if groups and not data_nodes:
        for group in groups:
            for node in group.nodes:
                if isinstance(node, orm.Data) and node not in data_nodes:
                    data_nodes.append(node)

    if not data_nodes and not groups:
        raise click.UsageError(
            'Nothing to export: specify `--data`, `--data-uuid`, `--group`, `--all-data` and/or `--all-groups`.'
        )

    try:
        export_shared_collection(
            data_nodes=data_nodes,
            groups=groups,
            output=output,
            shared_name=name,
            verify=verify,
        )
    except (ValueError, FileNotFoundError, TypeError) as exception:
        _raise_click_exception(exception)

    click.echo(f'Success: shared collection manifest written to `{output}`.')
    click.echo(f'  Data nodes: {len(data_nodes)}')
    click.echo(f'  Groups: {len(groups)}')


@cmd_collection.command('import')
@click.argument('manifest', type=click.Path(exists=True, path_type=pathlib.Path))
@click.option(
    '-n', '--name',
    'shared_name',
    default=None,
    help='Name of the shared repository (default: use name from manifest).',
)
@click.option(
    '--register',
    is_flag=True,
    help='Register the shared repository from the manifest before importing, prompting for credentials.',
)
@click.option(
    '--label-prefix',
    default='',
    help='Prefix to add to group labels (to avoid conflicts with existing groups).',
)
@decorators.with_dbenv()
def cmd_collection_import(
    manifest: pathlib.Path,
    shared_name: str | None,
    register: bool,
    label_prefix: str,
) -> None:
    """Import a shared collection manifest into the current profile.

    Creates new Data nodes and Groups, with repository files referenced by key
    in the shared S3 bucket. No object bytes are uploaded or downloaded: the
    nodes are created empty with their repository metadata pre-armed at the
    shared keys (one DB insert per node, zero S3 content requests).
    """
    from aiida_s3.shared_collection import COLLECTION_FORMAT, import_shared_collection

    # Read manifest to get source info for registration
    manifest_data = json.loads(manifest.read_text())
    if manifest_data.get('format') != COLLECTION_FORMAT:
        raise click.ClickException(f'`{manifest}` is not a shared collection manifest (expected format: {COLLECTION_FORMAT}).')

    credentials: dict[str, str] | None = None

    if register:
        source = manifest_data.get('source', {})
        name = shared_name or manifest_data.get('shared_name', 'shared')
        credentials = {
            'access_key_id': click.prompt(f'Access key ID for shared repository `{name}`', hide_input=True),
            'secret_access_key': click.prompt('Secret access key', hide_input=True),
        }

    try:
        result = import_shared_collection(
            manifest_path=manifest,
            shared_name=shared_name,
            credentials=credentials,
            label_prefix=label_prefix,
        )
    except (ValueError, FileNotFoundError) as exception:
        _raise_click_exception(exception)

    click.echo(f'Success: imported shared collection from `{manifest}`.')
    click.echo(f'  Data nodes: {len(result["data_nodes"])}')
    click.echo(f'  Groups: {len(result["groups"])}')
    for group in result['groups']:
        click.echo(f'    Group: {group.label} ({len(group.nodes)} members)')
