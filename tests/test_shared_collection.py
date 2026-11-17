"""Test for the generic shared collection export/import with real pseudopotentials.

This test verifies the abstract Data + Group export/import functionality using:
- Real UPF pseudopotential files from the Quantum ESPRESSO distribution
- UpfData nodes (aiida_pseudo.data.pseudo.upf.UpfData)
- SsspFamily group (aiida_pseudo.groups.family.sssp.SsspFamily) with cutoffs

The test creates two profiles:
- user2 (exporter): creates a pseudo family and exports it as a shared collection
- user1 (importer): imports the shared collection with user2's bucket as shared S3

Verifications:
1. user1's S3 bucket contains NO objects (no data duplication)
2. The imported family is fully functional (get_pseudo, get_cutoffs)
3. Content can be read via the aiida API
4. The shared objects are in user2's bucket (read-only)

Requires a real S3 server (SeaweedFS):
    AIIDA_S3_MOCK_S3=False
    AIIDA_S3_ENDPOINT_URL=http://localhost:8333
    AIIDA_S3_ACCESS_KEY_ID=minioadmin
    AIIDA_S3_SECRET_ACCESS_KEY=minioadmin
"""

from __future__ import annotations

import os
import pathlib
import typing as t
import uuid

import boto3
import pytest

pytestmark = pytest.mark.skipif(
    os.getenv('AIIDA_S3_MOCK_S3', 'True') != 'False',
    reason='This integration test requires a real S3 server (set AIIDA_S3_MOCK_S3=False)'
)

# Path to real UPF pseudopotential files from Quantum ESPRESSO
UPF_DIR = pathlib.Path('/home/bertossa/q-e/pseudo')

# Test elements and their UPF files (small files for fast testing)
TEST_PSEUDOS = {
    'H': 'H.blyp-vbc.UPF',      # 27 KB
    'C': 'C.pbe-rrkjus.UPF',    # 264 KB
    'Si': 'Si.pbe-rrkj.UPF',    # 169 KB
    'N': 'N.blyp-mt.UPF',       # 43 KB
}

# Cutoffs for the test family (eV)
TEST_CUTOFFS = {
    'H': {'cutoff_wfc': 30.0, 'cutoff_rho': 120.0},
    'C': {'cutoff_wfc': 50.0, 'cutoff_rho': 200.0},
    'Si': {'cutoff_wfc': 40.0, 'cutoff_rho': 160.0},
    'N': {'cutoff_wfc': 45.0, 'cutoff_rho': 180.0},
}


def get_s3_client() -> boto3.client:
    """Return a boto3 S3 client connected to the test S3 server."""
    return boto3.client(
        's3',
        endpoint_url=os.getenv('AIIDA_S3_ENDPOINT_URL', 'http://localhost:8333'),
        aws_access_key_id=os.getenv('AIIDA_S3_ACCESS_KEY_ID', 'minioadmin'),
        aws_secret_access_key=os.getenv('AIIDA_S3_SECRET_ACCESS_KEY', 'minioadmin'),
        region_name='us-east-1',
    )


def list_bucket_objects(client: boto3.client, bucket: str) -> list[str]:
    """Return list of all object keys in a bucket."""
    try:
        response = client.list_objects_v2(Bucket=bucket)
        return [obj['Key'] for obj in response.get('Contents', [])]
    except client.exceptions.NoSuchBucket:
        return []


def create_test_buckets(client: boto3.client, *bucket_names: str) -> None:
    """Create test buckets, ignoring if they already exist."""
    for bucket in bucket_names:
        try:
            client.create_bucket(Bucket=bucket)
        except client.exceptions.BucketAlreadyOwnedByYou:
            pass


def cleanup_test_buckets(client: boto3.client, *bucket_names: str) -> None:
    """Delete all objects and buckets."""
    for bucket in bucket_names:
        try:
            paginator = client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket):
                if 'Contents' in page:
                    objects = [{'Key': obj['Key']} for obj in page['Contents']]
                    client.delete_objects(Bucket=bucket, Delete={'Objects': objects})
            client.delete_bucket(Bucket=bucket)
        except Exception:
            pass


@pytest.fixture(scope='module')
def s3_test_setup():
    """Setup S3 buckets for the test."""
    client = get_s3_client()
    test_id = str(uuid.uuid4())[:8]
    buckets = {
        'user1': f'aiida-coll-user1-{test_id}',
        'user2': f'aiida-coll-user2-{test_id}',
    }
    create_test_buckets(client, *buckets.values())
    yield client, buckets
    cleanup_test_buckets(client, *buckets.values())


@pytest.fixture(scope='module')
def s3_test_config(s3_test_setup) -> dict[str, t.Any]:
    """Return S3 configuration for profiles."""
    client, buckets = s3_test_setup
    return {
        'endpoint_url': os.getenv('AIIDA_S3_ENDPOINT_URL', 'http://localhost:8333'),
        'access_key_id': os.getenv('AIIDA_S3_ACCESS_KEY_ID', 'minioadmin'),
        'secret_access_key': os.getenv('AIIDA_S3_SECRET_ACCESS_KEY', 'minioadmin'),
        '_buckets': buckets,
    }


@pytest.mark.skipif(not UPF_DIR.exists(), reason=f'UPF directory not found: {UPF_DIR}')
class TestSharedCollection:
    """Test generic shared collection export/import with real pseudopotentials."""

    def test_pseudo_family_shared_collection(
        self,
        s3_test_config: dict[str, t.Any],
        config_psql_dos,
        aiida_config,
        aiida_profile_factory,
        tmp_path_factory,
    ):
        """Test full workflow: export SSSP family from user2, import to user1 via shared S3."""
        from aiida import orm
        from aiida.manage import get_manager
        from aiida_pseudo.data.pseudo.upf import UpfData
        from aiida_pseudo.groups.family.sssp import SsspFamily
        from aiida_s3.shared_collection import export_shared_collection, import_shared_collection
        from aiida_s3.repository.multi import MultiS3RepositoryBackend

        buckets = s3_test_config['_buckets']
        s3_client = get_s3_client()
        tmp_path = tmp_path_factory.mktemp('shared_collection_test')

        shared_name = 'common-pseudos'
        family_label = 'SSSP/1.3/PBE/efficiency'

        # ============================================================
        # PHASE 1: user2 creates pseudo family from real UPF files
        # ============================================================
        print('\n' + '=' * 70)
        print('PHASE 1: user2 creates pseudo family from real UPF files')
        print('=' * 70)

        user2_config = config_psql_dos()
        user2_config.update({
            'endpoint_url': s3_test_config['endpoint_url'],
            'access_key_id': s3_test_config['access_key_id'],
            'secret_access_key': s3_test_config['secret_access_key'],
            'bucket_name': buckets['user2'],
        })

        pseudos_created = {}

        with aiida_profile_factory(
            aiida_config,
            storage_backend='s3.psql_s3',
            storage_config=user2_config,
            name='user2-collection-test',
        ) as user2_profile:
            # Create UpfData nodes from real UPF files
            for element, filename in TEST_PSEUDOS.items():
                upf_path = UPF_DIR / filename
                if not upf_path.exists():
                    print(f'  WARNING: {upf_path} not found, skipping {element}')
                    continue

                # Create UpfData node
                pseudo = UpfData(upf_path)
                pseudo.store()
                pseudos_created[element] = pseudo
                print(f'  Created {element} pseudopotential: PK={pseudo.pk}, UUID={pseudo.uuid[:8]}...')

            # Create SsspFamily group
            family = SsspFamily(label=family_label)
            family.description = 'Test SSSP family for shared collection'
            family.store()

            # Add pseudos to family
            family.add_nodes(list(pseudos_created.values()))

            # Set cutoffs
            family.set_cutoffs(TEST_CUTOFFS, stringency='efficiency', unit='eV')
            print(f'  Created family: {family.label}')
            print(f'  Family elements: {family.elements}')
            print(f'  Cutoffs: {family.get_cutoffs("efficiency")}')

            # Verify objects are in user2's bucket
            user2_repo = get_manager().get_profile_storage().get_repository()
            user2_objects = list(user2_repo.list_objects())
            print(f'\n  user2 bucket objects: {len(user2_objects)}')

            # S3 verification
            s3_objects_user2 = list_bucket_objects(s3_client, buckets['user2'])
            print(f'  S3 user2 bucket objects: {len(s3_objects_user2)}')
            assert len(s3_objects_user2) > 0, 'user2 bucket should have objects'

            # ============================================================
            # PHASE 2: Export family as shared collection
            # ============================================================
            print('\n' + '=' * 70)
            print('PHASE 2: Export family as shared collection')
            print('=' * 70)

            manifest_path = tmp_path / 'pseudo_family.json'
            export_shared_collection(
                data_nodes=list(pseudos_created.values()),
                groups=[family],
                output=manifest_path,
                shared_name=shared_name,
            )
            print(f'  Manifest created: {manifest_path}')
            print(f'  Manifest size: {manifest_path.stat().st_size} bytes')

            # Verify manifest content
            import json
            manifest = json.loads(manifest_path.read_text())
            print(f'  Format: {manifest["format"]}')
            print(f'  Data nodes: {len(manifest["data_nodes"])}')
            print(f'  Groups: {len(manifest["groups"])}')
            assert manifest['format'] == 'aiida-s3-shared-collection/1'
            assert len(manifest['data_nodes']) == len(pseudos_created)
            assert len(manifest['groups']) == 1

        # ============================================================
        # PHASE 3: user1 imports shared collection
        # ============================================================
        print('\n' + '=' * 70)
        print('PHASE 3: user1 imports shared collection')
        print('=' * 70)

        user1_config = config_psql_dos()
        user1_config.update({
            'endpoint_url': s3_test_config['endpoint_url'],
            'access_key_id': s3_test_config['access_key_id'],
            'secret_access_key': s3_test_config['secret_access_key'],
            'bucket_name': buckets['user1'],
            'shared_repositories': [
                {
                    'name': shared_name,
                    'endpoint_url': s3_test_config['endpoint_url'],
                    'access_key_id': s3_test_config['access_key_id'],
                    'secret_access_key': s3_test_config['secret_access_key'],
                    'bucket_name': buckets['user2'],
                }
            ],
        })

        with aiida_profile_factory(
            aiida_config,
            storage_backend='s3.psql_s3',
            storage_config=user1_config,
            name='user1-collection-test',
        ) as user1_profile:
            user1_repo = get_manager().get_profile_storage().get_repository()
            assert isinstance(user1_repo, MultiS3RepositoryBackend)

            # Verify shared repository
            assert user1_repo.verify_shared() == {shared_name: True}
            print(f'  Shared repository verified: {shared_name}')

            # Check user1's bucket is empty before import
            user1_objects_before = list(user1_repo.list_objects())
            print(f'  user1 bucket objects before import: {len(user1_objects_before)}')
            assert len(user1_objects_before) == 0

            # Import the collection
            result = import_shared_collection(
                manifest_path=manifest_path,
                shared_name=shared_name,
            )
            print(f'  Imported {len(result["data_nodes"])} data nodes')
            print(f'  Imported {len(result["groups"])} groups')

            # ============================================================
            # PHASE 4: Verify NO data duplication
            # ============================================================
            print('\n' + '=' * 70)
            print('PHASE 4: Verify NO data duplication')
            print('=' * 70)

            # user1's bucket should still be empty
            user1_objects_after = list(user1_repo.list_objects())
            print(f'  user1 bucket objects after import: {len(user1_objects_after)}')
            assert len(user1_objects_after) == 0, \
                f'user1 bucket should be empty, has {len(user1_objects_after)} objects'

            # S3 verification
            s3_objects_user1 = list_bucket_objects(s3_client, buckets['user1'])
            print(f'  S3 user1 bucket objects: {len(s3_objects_user1)}')
            assert len(s3_objects_user1) == 0, \
                f'S3 user1 bucket should be empty, has: {s3_objects_user1}'

            # user2's bucket should still have all objects
            s3_objects_user2_after = list_bucket_objects(s3_client, buckets['user2'])
            print(f'  S3 user2 bucket objects: {len(s3_objects_user2_after)}')
            assert len(s3_objects_user2_after) > 0, 'user2 bucket should still have objects'

            print('  ✓ NO data duplication - user1 bucket is empty!')

            # ============================================================
            # PHASE 5: Verify imported family is fully functional
            # ============================================================
            print('\n' + '=' * 70)
            print('PHASE 5: Verify imported family is fully functional')
            print('=' * 70)

            imported_family = result['groups'][0]
            print(f'  Imported family: {imported_family.label}')
            print(f'  Family type: {type(imported_family).__name__}')
            print(f'  Family elements: {imported_family.elements}')

            # Verify family has all elements
            assert set(imported_family.elements) == set(pseudos_created.keys())

            # Verify cutoffs are preserved
            cutoffs = imported_family.get_cutoffs('efficiency')
            print(f'  Cutoffs: {cutoffs}')
            for element, expected in TEST_CUTOFFS.items():
                assert element in cutoffs, f'Missing cutoffs for {element}'
                assert cutoffs[element]['cutoff_wfc'] == expected['cutoff_wfc']
                assert cutoffs[element]['cutoff_rho'] == expected['cutoff_rho']
            print('  ✓ Cutoffs preserved')

            # Verify get_pseudo works
            for element in TEST_PSEUDOS.keys():
                pseudo = imported_family.get_pseudo(element)
                print(f'  get_pseudo({element}): PK={pseudo.pk}, type={type(pseudo).__name__}')
                assert pseudo.element == element

            # Verify content can be read via aiida API
            print('\n  Verifying content via aiida API:')
            for element in TEST_PSEUDOS.keys():
                pseudo = imported_family.get_pseudo(element)
                with pseudo.open(mode='rb') as handle:
                    content = handle.read()
                original_path = UPF_DIR / TEST_PSEUDOS[element]
                original_content = original_path.read_bytes()
                assert content == original_content, f'Content mismatch for {element}'
                print(f'    {element}: {len(content)} bytes ✓ matches original')

            # Verify get_recommended_cutoffs works
            wfc, rho = imported_family.get_recommended_cutoffs(elements=('Si', 'C'))
            print(f'\n  Recommended cutoffs for [Si, C]: wfc={wfc} eV, rho={rho} eV')
            assert wfc == max(TEST_CUTOFFS['Si']['cutoff_wfc'], TEST_CUTOFFS['C']['cutoff_wfc'])
            assert rho == max(TEST_CUTOFFS['Si']['cutoff_rho'], TEST_CUTOFFS['C']['cutoff_rho'])

            # ============================================================
            # PHASE 6: Verify shared objects are read-only
            # ============================================================
            print('\n' + '=' * 70)
            print('PHASE 6: Verify shared objects are read-only')
            print('=' * 70)

            pseudo = imported_family.get_pseudo('Si')
            metadata = pseudo.backend_entity.repository_metadata or {}
            from aiida_s3.shared_collection import iter_repository_keys
            from aiida_s3.repository.multi import split_shared_key
            shared_key = list(dict(iter_repository_keys(metadata)).values())[0]

            # Default soft-delete: deleting a shared object removes only the reference,
            # the S3 object in the shared bucket is preserved (soft_delete_shared)
            user1_repo.delete_objects([shared_key])
            _, object_key = split_shared_key(shared_key)
            s3_object_keys_user2 = list_bucket_objects(s3_client, buckets['user2'])
            assert object_key in s3_object_keys_user2, 'shared S3 object must be preserved by soft delete'
            print('  ✓ Soft delete of shared object preserves the S3 object in the shared bucket')

            # ============================================================
            # SUMMARY
            # ============================================================
            print('\n' + '=' * 70)
            print('TEST SUMMARY')
            print('=' * 70)
            print(f'  user1 bucket ({buckets["user1"]}): {len(list_bucket_objects(s3_client, buckets["user1"]))} objects (should be 0)')
            print(f'  user2 bucket ({buckets["user2"]}): {len(list_bucket_objects(s3_client, buckets["user2"]))} objects (shared, read-only)')
            print(f'  Family: {imported_family.label}')
            print(f'  Elements: {imported_family.elements}')
            print(f'  Cutoffs: preserved ✓')
            print(f'  Content: readable via aiida API ✓')
            print(f'  No data duplication: ✓')
            print(f'  Shared object preservation on delete: ✓')
            print('=' * 70)
            print('ALL TESTS PASSED!')
            print('=' * 70)

    def test_export_import_arbitrary_data_and_group(
        self,
        s3_test_config: dict[str, t.Any],
        config_psql_dos,
        aiida_config,
        aiida_profile_factory,
        tmp_path_factory,
    ):
        """Test with arbitrary Data nodes and a plain Group (not pseudo-specific)."""
        from aiida import orm
        from aiida.manage import get_manager
        from aiida_s3.shared_collection import export_shared_collection, import_shared_collection

        buckets = s3_test_config['_buckets']
        s3_client = get_s3_client()
        tmp_path = tmp_path_factory.mktemp('arbitrary_collection_test')
        shared_name = 'arbitrary-shared'

        # Phase 1: Create arbitrary Data nodes and a Group in user2
        user2_config = config_psql_dos()
        user2_config.update({
            'endpoint_url': s3_test_config['endpoint_url'],
            'access_key_id': s3_test_config['access_key_id'],
            'secret_access_key': s3_test_config['secret_access_key'],
            'bucket_name': buckets['user2'],
        })

        with aiida_profile_factory(
            aiida_config,
            storage_backend='s3.psql_s3',
            storage_config=user2_config,
            name='user2-arbitrary-test',
        ):
            # Create various Data nodes
            import io
            dict_node = orm.Dict(dict={'key': 'value', 'number': 42})
            dict_node.label = 'Test Dict'
            dict_node.store()

            singlefile_node = orm.SinglefileData(io.BytesIO(b'test file content'), filename='test.txt')
            singlefile_node.store()

            # Create a plain Group
            group = orm.Group(label='test-arbitrary-group')
            group.description = 'A test group'
            group.store()
            group.add_nodes([dict_node, singlefile_node])

            # Export
            manifest_path = tmp_path / 'arbitrary.json'
            export_shared_collection(
                data_nodes=[dict_node, singlefile_node],
                groups=[group],
                output=manifest_path,
                shared_name=shared_name,
            )
            print(f'Exported arbitrary collection: {manifest_path}')

        # Phase 2: Import in user1
        user1_config = config_psql_dos()
        user1_config.update({
            'endpoint_url': s3_test_config['endpoint_url'],
            'access_key_id': s3_test_config['access_key_id'],
            'secret_access_key': s3_test_config['secret_access_key'],
            'bucket_name': buckets['user1'],
            'shared_repositories': [
                {
                    'name': shared_name,
                    'endpoint_url': s3_test_config['endpoint_url'],
                    'access_key_id': s3_test_config['access_key_id'],
                    'secret_access_key': s3_test_config['secret_access_key'],
                    'bucket_name': buckets['user2'],
                }
            ],
        })

        with aiida_profile_factory(
            aiida_config,
            storage_backend='s3.psql_s3',
            storage_config=user1_config,
            name='user1-arbitrary-test',
        ):
            result = import_shared_collection(
                manifest_path=manifest_path,
                shared_name=shared_name,
            )

            assert len(result['data_nodes']) == 2
            assert len(result['groups']) == 1

            # Verify user1 bucket is empty
            user1_repo = get_manager().get_profile_storage().get_repository()
            assert len(list(user1_repo.list_objects())) == 0

            # Verify group has members
            imported_group = result['groups'][0]
            assert len(imported_group.nodes) == 2
            print(f'Imported group: {imported_group.label} with {len(imported_group.nodes)} members')

            # Verify content readable
            imported_singlefile = [n for n in result['data_nodes'] if isinstance(n, orm.SinglefileData)][0]
            with imported_singlefile.open(mode='rb') as handle:
                assert handle.read() == b'test file content'
            print('Content verified ✓')

            print('ARBITRARY COLLECTION TEST PASSED!')

    def test_import_with_explicit_backend_no_global_profile(
        self,
        s3_test_config: dict[str, t.Any],
        config_psql_dos,
        aiida_config,
        aiida_profile_factory,
        tmp_path_factory,
    ):
        """Import must work with an explicit storage backend while the target profile is NOT the globally loaded one.

        This is the DashboardServer temporal worker mode: the worker manages its own aiida
        Managers and passes the storage backend explicitly (the aiidasdk AiidaConnectionManager
        pattern), so the import must never consult the global profile. The globally loaded
        profile during the import is the unrelated session test profile: if any code path
        fell back to it, nodes would land in the wrong storage or the global profile would
        be switched.
        """
        from aiida import orm
        from aiida.manage import get_manager
        from aiida.manage.configuration import get_profile
        from aiida.manage.configuration.profile import Profile
        from aiida.manage.manager import Manager
        from aiida_pseudo.data.pseudo.upf import UpfData
        from aiida_pseudo.groups.family.sssp import SsspFamily
        from aiida_s3.repository.multi import MultiS3RepositoryBackend
        from aiida_s3.shared_collection import export_shared_collection, import_shared_collection

        buckets = s3_test_config['_buckets']
        s3_client = get_s3_client()
        tmp_path = tmp_path_factory.mktemp('explicit_backend_test')
        shared_name = 'explicit-shared'
        family_label = 'SSSP/1.3/PBE/efficiency'

        # Phase 1: user2 creates the pseudo family and exports it with an explicit backend
        user2_config = config_psql_dos()
        user2_config.update({
            'endpoint_url': s3_test_config['endpoint_url'],
            'access_key_id': s3_test_config['access_key_id'],
            'secret_access_key': s3_test_config['secret_access_key'],
            'bucket_name': buckets['user2'],
        })

        with aiida_profile_factory(
            aiida_config,
            storage_backend='s3.psql_s3',
            storage_config=user2_config,
            name='user2-explicit-test',
        ) as user2_profile:
            pseudos_created = {}
            for element, filename in TEST_PSEUDOS.items():
                upf_path = UPF_DIR / filename
                if not upf_path.exists():
                    print(f'  WARNING: {upf_path} not found, skipping {element}')
                    continue
                pseudo = UpfData(upf_path)
                pseudo.store()
                pseudos_created[element] = pseudo

            family = SsspFamily(label=family_label)
            family.description = 'Family exported with an explicit backend'
            family.store()
            family.add_nodes(list(pseudos_created.values()))
            family.set_cutoffs(TEST_CUTOFFS, stringency='efficiency', unit='eV')

            manifest_path = tmp_path / 'explicit.json'
            export_shared_collection(
                data_nodes=list(pseudos_created.values()),
                groups=[family],
                output=manifest_path,
                shared_name=shared_name,
                backend=get_manager().get_profile_storage(),
            )
            print(f'  Exported {len(pseudos_created)} nodes with explicit backend')

        # Phase 2: user1 target profile - create and initialise it, then leave the profile
        # context: the global profile is restored to the unrelated session test profile
        user1_config = config_psql_dos()
        user1_config.update({
            'endpoint_url': s3_test_config['endpoint_url'],
            'access_key_id': s3_test_config['access_key_id'],
            'secret_access_key': s3_test_config['secret_access_key'],
            'bucket_name': buckets['user1'],
            'shared_repositories': [
                {
                    'name': shared_name,
                    'endpoint_url': s3_test_config['endpoint_url'],
                    'access_key_id': s3_test_config['access_key_id'],
                    'secret_access_key': s3_test_config['secret_access_key'],
                    'bucket_name': buckets['user2'],
                }
            ],
        })

        with aiida_profile_factory(
            aiida_config,
            storage_backend='s3.psql_s3',
            storage_config=user1_config,
            name='user1-explicit-test',
        ) as user1_profile:
            user1_dictionary = dict(user1_profile.dictionary)

        global_profile = get_profile()
        assert global_profile is not None, 'the session test profile should be loaded globally'
        assert global_profile.name != user1_profile.name, 'the target profile must NOT be the global one'
        global_backend = get_manager().get_profile_storage()
        nodes_in_global_storage_before = orm.QueryBuilder(backend=global_backend).append(orm.Node).count()

        # Phase 3: import through a private Manager over the target profile, the
        # aiidasdk AiidaConnectionManager pattern: no global profile involved
        manager = Manager()
        manager.load_profile(Profile(user1_profile.name, user1_dictionary))
        try:
            backend = manager.get_profile_storage()
            assert isinstance(backend.get_repository(), MultiS3RepositoryBackend)
            assert backend is not global_backend

            # credentials registration is incompatible with an explicit backend
            with pytest.raises(ValueError, match='credentials'):
                import_shared_collection(
                    manifest_path=manifest_path,
                    shared_name=shared_name,
                    credentials={'access_key_id': 'x', 'secret_access_key': 'y'},
                    backend=backend,
                )

            result = import_shared_collection(
                manifest_path=manifest_path,
                shared_name=shared_name,
                backend=backend,
            )

            # the import must not have switched or touched the global profile
            assert get_profile() is global_profile, 'import must not switch the global profile'
            assert get_manager().get_profile_storage() is global_backend
            nodes_in_global_storage_after = orm.QueryBuilder(backend=global_backend).append(orm.Node).count()
            assert nodes_in_global_storage_after == nodes_in_global_storage_before, \
                'import must not write anything to the storage of the global profile'

            assert len(result['data_nodes']) == len(pseudos_created)
            assert len(result['groups']) == 1

            # no data duplication: user1's bucket stays empty
            assert len(list(backend.get_repository().list_objects())) == 0
            assert len(list_bucket_objects(s3_client, buckets['user1'])) == 0

            # the shared bucket still holds the objects
            assert len(list_bucket_objects(s3_client, buckets['user2'])) > 0

            # reload the family through the explicit backend only and verify it is functional
            qb = orm.QueryBuilder(backend=backend)
            qb.append(SsspFamily, filters={'label': family_label})
            imported_family = qb.one()[0]
            assert set(imported_family.elements) == set(pseudos_created.keys())
            assert imported_family.get_cutoffs('efficiency')['Si']['cutoff_wfc'] == TEST_CUTOFFS['Si']['cutoff_wfc']

            pseudo = imported_family.get_pseudo('Si')
            with pseudo.open(mode='rb') as handle:
                assert handle.read() == (UPF_DIR / TEST_PSEUDOS['Si']).read_bytes()
            print('  Content read through the shared bucket via the explicit backend ✓')
        finally:
            manager.unload_profile()

        assert get_profile() is global_profile
        print('EXPLICIT BACKEND (NO GLOBAL PROFILE) TEST PASSED!')

    def test_import_bypasses_md5_revalidation_no_content_read(
        self,
        s3_test_config: dict[str, t.Any],
        config_psql_dos,
        aiida_config,
        aiida_profile_factory,
        tmp_path_factory,
        monkeypatch,
    ):
        """The metadata-only import must not read any file content while storing.

        aiida-pseudo's store() re-validates the md5 by re-reading the file,
        which for a shared repository means downloading the object from the
        shared bucket. The import bypasses that override via super() (no
        monkey-patching), so during the import: validate_md5 must never be
        called and the repository must never open() an object. If the bypass
        regressed, both spies below would catch it while still delegating to
        the real implementations (the import would succeed, but the test
        would see the calls).
        """
        import io

        from aiida.manage import get_manager
        from aiida.manage.configuration import get_profile
        from aiida.manage.configuration.profile import Profile
        from aiida.manage.manager import Manager
        from aiida_pseudo.data.pseudo import upf
        from aiida_s3.repository import multi as multi_repository_module
        from aiida_s3.shared_collection import export_shared_collection, import_shared_collection

        buckets = s3_test_config['_buckets']
        tmp_path = tmp_path_factory.mktemp('bypass_md5_test')
        shared_name = 'bypass-md5-shared'

        # Phase 1: export one UpfData node from user2
        user2_config = config_psql_dos()
        user2_config.update({
            'endpoint_url': s3_test_config['endpoint_url'],
            'access_key_id': s3_test_config['access_key_id'],
            'secret_access_key': s3_test_config['secret_access_key'],
            'bucket_name': buckets['user2'],
        })

        with aiida_profile_factory(
            aiida_config,
            storage_backend='s3.psql_s3',
            storage_config=user2_config,
            name='user2-bypass-md5-test',
        ):
            upf_path = UPF_DIR / TEST_PSEUDOS['H']
            source_pseudo = upf.UpfData(upf_path)
            source_pseudo.store()
            source_md5 = source_pseudo.md5

            manifest_path = tmp_path / 'bypass.json'
            export_shared_collection(
                data_nodes=[source_pseudo],
                groups=None,
                output=manifest_path,
                shared_name=shared_name,
            )

        # Phase 2: prepare the user1 profile with the shared repository
        user1_config = config_psql_dos()
        user1_config.update({
            'endpoint_url': s3_test_config['endpoint_url'],
            'access_key_id': s3_test_config['access_key_id'],
            'secret_access_key': s3_test_config['secret_access_key'],
            'bucket_name': buckets['user1'],
            'shared_repositories': [
                {
                    'name': shared_name,
                    'endpoint_url': s3_test_config['endpoint_url'],
                    'access_key_id': s3_test_config['access_key_id'],
                    'secret_access_key': s3_test_config['secret_access_key'],
                    'bucket_name': buckets['user2'],
                }
            ],
        })

        with aiida_profile_factory(
            aiida_config,
            storage_backend='s3.psql_s3',
            storage_config=user1_config,
            name='user1-bypass-md5-test',
        ) as user1_profile:
            user1_dictionary = dict(user1_profile.dictionary)

        # spies: count the calls, delegate to the real implementations
        calls: dict[str, int] = {'validate_md5': 0, 'open': 0}

        pseudo_base = type(source_pseudo).__mro__[
            next(i for i, cls in enumerate(type(source_pseudo).__mro__) if 'validate_md5' in vars(cls))
        ]
        original_validate = pseudo_base.validate_md5
        original_open = multi_repository_module.MultiS3RepositoryBackend.open

        def spy_validate(instance, md5):
            calls['validate_md5'] += 1
            return original_validate(instance, md5)

        def spy_open(instance, *args, **kwargs):
            calls['open'] += 1
            return original_open(instance, *args, **kwargs)

        monkeypatch.setattr(pseudo_base, 'validate_md5', spy_validate)
        monkeypatch.setattr(multi_repository_module.MultiS3RepositoryBackend, 'open', spy_open)

        # Phase 3: import through a private Manager (the temporal worker mode)
        global_profile = get_profile()
        manager = Manager()
        manager.load_profile(Profile(user1_profile.name, user1_dictionary))
        try:
            backend = manager.get_profile_storage()

            result = import_shared_collection(
                manifest_path=manifest_path,
                shared_name=shared_name,
                backend=backend,
            )

            assert get_profile() is global_profile

            assert len(result['data_nodes']) == 1
            imported = result['data_nodes'][0]

            # THE assertion: nothing re-read the file content during the import
            assert calls['validate_md5'] == 0, 'validate_md5 must be bypassed on import'
            assert calls['open'] == 0, 'the repository must not open (download) any object on import'

            # the stored node carries the exported md5 and the file stays readable
            assert imported.md5 == source_md5
            with imported.open(mode='rb') as handle:
                assert handle.read() == upf_path.read_bytes()
            print('  No content read during import; content still readable through the shared bucket')
        finally:
            manager.unload_profile()
        print('IMPORT BYPASSES MD5 REVALIDATION (NO CONTENT READ) TEST PASSED!')
