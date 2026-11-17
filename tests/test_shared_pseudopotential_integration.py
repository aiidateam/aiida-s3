"""Integration test for the multi-S3 shared repository feature with pseudopotential data.

This test verifies the complete workflow requested:

1. Setup clean aiida repository for user1
2. Setup clean aiida repo for user2
3. Create pseudopotential data in user2's profile and store in user2's S3 bucket
4. Export pseudopotential data as a shared archive (byte-free)
5. Import the shared archive into user1's profile (which has user2's bucket as shared)
6. Verify that user1's S3 bucket contains NO copies of the shared objects
7. Use the aiida API to retrieve file content from the shared S3 and verify it works

This test requires a real S3 server (SeaweedFS) running locally.
Set environment variables:
    AIIDA_S3_MOCK_S3=False
    AIIDA_S3_ENDPOINT_URL=http://localhost:8333
    AIIDA_S3_ACCESS_KEY_ID=minioadmin
    AIIDA_S3_SECRET_ACCESS_KEY=minioadmin
"""

from __future__ import annotations

import io
import os
import pathlib
import tempfile
import typing as t
import uuid

import boto3
import pytest

pytestmark = pytest.mark.skipif(
    os.getenv('AIIDA_S3_MOCK_S3', 'True') != 'False',
    reason='This integration test requires a real S3 server (set AIIDA_S3_MOCK_S3=False)'
)


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
            # Delete all objects
            paginator = client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket):
                if 'Contents' in page:
                    objects = [{'Key': obj['Key']} for obj in page['Contents']]
                    client.delete_objects(Bucket=bucket, Delete={'Objects': objects})
            # Delete bucket
            client.delete_bucket(Bucket=bucket)
        except Exception:
            pass


@pytest.fixture(scope='module')
def s3_test_setup():
    """Setup S3 buckets for the test."""
    client = get_s3_client()
    test_id = str(uuid.uuid4())[:8]
    buckets = {
        'user1': f'aiida-user1-{test_id}',
        'user2': f'aiida-user2-{test_id}',
        'shared': f'aiida-shared-{test_id}',
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


def make_pseudopotential_content(element: str, functional: str = 'PBE') -> bytes:
    """Generate fake pseudopotential file content for testing."""
    return f"""&CONTROL
    calculation = 'scf'
/
&ELECTRONS
    conv_thr = 1.0d-8
/
&SYSTEM
    ibrav = 1
    celldm(1) = 10.0
    nat = 1
    ntyp = 1
    ecutwfc = 30.0
    ecutrho = 240.0
/
ATOMIC_SPECIES
    {element}  1.00  {element}.UPF
ATOMIC_POSITIONS alat
    {element}  0.0  0.0  0.0
K_POINTS automatic
    4 4 4  0 0 0
""".encode()


def make_pseudopotential_upf(element: str, functional: str = 'PBE') -> bytes:
    """Generate a minimal UPF format pseudopotential file for testing."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<UPF version="2.0.1">
    <PP_INFO>
        <PP_ELEMENT>{element}</PP_ELEMENT>
        <PP_FUNCTIONAL>{functional}</PP_FUNCTIONAL>
    </PP_INFO>
    <PP_HEADER
        generated="test"
        element="{element}"
        pseudo_type="NC"
        relativistic="scalar"
    />
    <PP_MESH>
        <PP_RAB type="linear" size="1000" dx="0.01"/>
        <PP_R type="linear" size="1000" dx="0.01"/>
    </PP_MESH>
    <PP_LOCAL>
        <PP_VLOC type="numeric" size="1000"/>
    </PP_LOCAL>
    <PP_NONLOCAL>
    </PP_NONLOCAL>
    <PP_PSWFC>
    </PP_PSWFC>
</UPF>
""".encode()


class TestSharedPseudopotentialIntegration:
    """Integration tests for shared S3 repository with pseudopotential data."""

    def test_full_workflow(
        self,
        s3_test_config: dict[str, t.Any],
        config_psql_dos,
        aiida_config,
        aiida_profile_factory,
        tmp_path_factory,
    ):
        """Test the complete workflow: user2 exports pseudopotentials, user1 imports them via shared S3.

        This test verifies:
        1. user2 creates pseudopotential data nodes in its own bucket
        2. user2 exports them as a shared archive (no bytes)
        3. user1 registers user2's bucket as a shared repository
        4. user1 imports the shared archive
        5. user1's bucket contains NO copies of the shared objects
        6. user1 can read the pseudopotential content via the aiida API
        """
        from aiida import orm
        from aiida.manage import get_manager
        from aiida_s3.shared import iter_repository_keys
        from aiida_s3.shared_io import export_shared, import_shared
        from aiida_s3.repository.multi import MultiS3RepositoryBackend, make_shared_key, split_shared_key

        buckets = s3_test_config['_buckets']
        s3_client = get_s3_client()

        # Create temporary directory for archives
        tmp_path = tmp_path_factory.mktemp('shared_pseudo_test')

        # ============================================================
        # PHASE 1: Setup user2 profile and create pseudopotential data
        # ============================================================
        print('\n' + '=' * 70)
        print('PHASE 1: Setup user2 profile and create pseudopotential data')
        print('=' * 70)

        # Build storage config with database + S3
        user2_config = config_psql_dos()
        user2_config.update({
            'endpoint_url': s3_test_config['endpoint_url'],
            'access_key_id': s3_test_config['access_key_id'],
            'secret_access_key': s3_test_config['secret_access_key'],
            'bucket_name': buckets['user2'],
        })

        pseudopotentials_created = {}

        with aiida_profile_factory(
            aiida_config,
            storage_backend='s3.psql_s3',
            storage_config=user2_config,
            name='user2-test',
        ) as user2_profile:
            # Create pseudopotential data nodes
            elements = ['Si', 'C', 'O', 'H', 'N']
            for element in elements:
                # Create a SinglefileData node with pseudopotential content
                upf_content = make_pseudopotential_upf(element)
                pseudo_file = tmp_path / f'{element}.UPF'
                pseudo_file.write_bytes(upf_content)

                # Create the data node
                pseudo_node = orm.SinglefileData(file=str(pseudo_file))
                pseudo_node.base.attributes.set('element', element)
                pseudo_node.base.attributes.set('functional', 'PBE')
                pseudo_node.store()

                # Also create an input file (QE input) that references the pseudopotential
                qe_input = make_pseudopotential_content(element)
                input_node = orm.SinglefileData(io.BytesIO(qe_input), filename=f'{element}_input.in')
                input_node.store()

                pseudopotentials_created[element] = {
                    'pseudo_node': pseudo_node,
                    'input_node': input_node,
                    'pseudo_uuid': pseudo_node.uuid,
                    'input_uuid': input_node.uuid,
                    'content': upf_content,
                    'qe_input': qe_input,
                }
                print(f'  Created pseudopotential for {element}: node PK={pseudo_node.pk}')

            # Get repository info for user2
            user2_repo = get_manager().get_profile_storage().get_repository()
            user2_keys = set(user2_repo.list_objects())
            print(f'\n  user2 bucket ({buckets["user2"]}) contains {len(user2_keys)} objects')

            # Verify objects exist in S3 directly
            s3_objects_user2 = set(list_bucket_objects(s3_client, buckets['user2']))
            print(f'  S3 bucket ({buckets["user2"]}) contains {len(s3_objects_user2)} objects')
            assert user2_keys == s3_objects_user2, 'Repository objects should match S3 objects'

            # Collect all nodes to export
            nodes_to_export = []
            for data in pseudopotentials_created.values():
                nodes_to_export.append(data['pseudo_node'])
                nodes_to_export.append(data['input_node'])

            # ============================================================
            # PHASE 2: Export pseudopotentials as shared archive
            # ============================================================
            print('\n' + '=' * 70)
            print('PHASE 2: Export pseudopotentials as shared archive')
            print('=' * 70)

            archive_path = tmp_path / 'pseudopotentials_shared.aiida'
            shared_name = 'common-pseudos'

            export_shared(shared_name, nodes_to_export, archive_path)
            print(f'  Shared archive created: {archive_path}')
            print(f'  Archive size: {archive_path.stat().st_size} bytes')

            # Verify archive contains no object bytes
            import zipfile
            with zipfile.ZipFile(archive_path, 'r') as zf:
                names = zf.namelist()
                print(f'  Archive contents: {names}')
                # Should only have metadata.json and db.sqlite3, NO repo/ files
                assert 'metadata.json' in names
                assert 'db.sqlite3' in names
                assert not any(n.startswith('repo/') for n in names), 'Archive should not contain repo/ files'

                # Check metadata
                import json
                metadata = json.loads(zf.read('metadata.json'))
                print(f'  Manifest: {metadata.get("shared_repository")}')
                assert metadata['shared_repository']['name'] == shared_name
                assert metadata['shared_repository']['bucket_name'] == buckets['user2']

        # ============================================================
        # PHASE 3: Setup user1 profile with user2's bucket as shared
        # ============================================================
        print('\n' + '=' * 70)
        print('PHASE 3: Setup user1 profile with user2 as shared repository')
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
            name='user1-test',
        ) as user1_profile:
            user1_repo = get_manager().get_profile_storage().get_repository()
            assert isinstance(user1_repo, MultiS3RepositoryBackend), 'Should be MultiS3RepositoryBackend'

            # Verify shared repository is registered and reachable
            shared_status = user1_repo.verify_shared()
            print(f'  Shared repositories status: {shared_status}')
            assert shared_status == {shared_name: True}

            # Check user1's bucket is empty before import
            user1_objects_before = set(user1_repo.list_objects())
            print(f'  user1 bucket objects before import: {len(user1_objects_before)}')
            assert len(user1_objects_before) == 0, 'user1 bucket should be empty before import'

            # S3 check
            s3_objects_user1_before = set(list_bucket_objects(s3_client, buckets['user1']))
            print(f'  S3 user1 bucket objects before import: {len(s3_objects_user1_before)}')
            assert len(s3_objects_user1_before) == 0

            # ============================================================
            # PHASE 4: Import shared archive into user1
            # ============================================================
            print('\n' + '=' * 70)
            print('PHASE 4: Import shared archive into user1')
            print('=' * 70)

            import_shared(archive_path)
            print('  Import completed!')

            # ============================================================
            # PHASE 5: Verify user1's bucket has NO copies of shared objects
            # ============================================================
            print('\n' + '=' * 70)
            print('PHASE 5: Verify user1 bucket has NO copies of shared objects')
            print('=' * 70)

            # Check user1's repository
            user1_objects_after = set(user1_repo.list_objects())
            print(f'  user1 bucket objects after import: {len(user1_objects_after)}')

            # CRITICAL: user1's bucket should still be empty!
            assert len(user1_objects_after) == 0, \
                f'user1 bucket should be empty after import, but has {len(user1_objects_after)} objects: {user1_objects_after}'

            # Check S3 directly
            s3_objects_user1_after = set(list_bucket_objects(s3_client, buckets['user1']))
            print(f'  S3 user1 bucket objects after import: {len(s3_objects_user1_after)}')
            assert len(s3_objects_user1_after) == 0, \
                f'S3 user1 bucket should be empty, but has: {s3_objects_user1_after}'

            # Verify the shared objects are still only in user2's bucket
            s3_objects_user2_after = set(list_bucket_objects(s3_client, buckets['user2']))
            print(f'  S3 user2 (shared) bucket objects: {len(s3_objects_user2_after)}')
            assert len(s3_objects_user2_after) > 0, 'user2 bucket should still have the objects'

            # Check that user1's objects are NOT in user1's bucket
            for obj_key in s3_objects_user2_after:
                assert obj_key not in s3_objects_user1_after, \
                    f'Object {obj_key} should NOT be in user1 bucket!'

            print('  ✓ user1 bucket contains NO copies of shared objects')

            # ============================================================
            # PHASE 6: Verify aiida API can read content from shared S3
            # ============================================================
            print('\n' + '=' * 70)
            print('PHASE 6: Verify aiida API can read content from shared S3')
            print('=' * 70)

            # Load imported nodes and verify content
            for element, data in pseudopotentials_created.items():
                # Load pseudopotential node
                pseudo_node = orm.load_node(data['pseudo_uuid'])
                print(f'\n  Loading {element} pseudopotential (UUID: {data["pseudo_uuid"][:8]}...)')

                # Get repository metadata keys
                repo_metadata = pseudo_node.backend_entity.repository_metadata or {}
                keys = dict(iter_repository_keys(repo_metadata))
                print(f'    Repository keys: {list(keys.values())}')

                # Verify keys are namespaced for shared repository
                for path, key in keys.items():
                    parts = split_shared_key(key)
                    assert parts is not None, f'Key should be shared-namespaced: {key}'
                    assert parts[0] == shared_name, f'Key should be for {shared_name}: {key}'
                    print(f'    {path} -> {key} (shared: {parts[0]}, object: {parts[1][:8]}...)')

                # Read content via aiida API
                with pseudo_node.open(mode='rb') as handle:
                    content = handle.read()
                    print(f'    Content size: {len(content)} bytes')
                    assert content == data['content'], f'Content mismatch for {element}'
                    print(f'    ✓ Content matches original ({element}.UPF)')

                # Also verify via get_object_content
                content_via_api = pseudo_node.base.repository.get_object_content(
                    list(keys.keys())[0], mode='rb'
                )
                assert content_via_api == data['content']
                print(f'    ✓ get_object_content API works for {element}')

                # Load QE input node and verify
                input_node = orm.load_node(data['input_uuid'])
                input_metadata = input_node.backend_entity.repository_metadata or {}
                input_keys = dict(iter_repository_keys(input_metadata))

                with input_node.open(mode='rb') as handle:
                    input_content = handle.read()
                    assert input_content == data['qe_input'], f'QE input mismatch for {element}'
                    print(f'    ✓ QE input content matches for {element}')

                # Verify attributes
                assert pseudo_node.base.attributes.get('element') == element
                assert pseudo_node.base.attributes.get('functional') == 'PBE'
                print(f'    ✓ Attributes preserved for {element}')

            # ============================================================
            # PHASE 7: Verify shared objects are readable but not deletable
            # ============================================================
            print('\n' + '=' * 70)
            print('PHASE 7: Verify shared objects are read-only')
            print('=' * 70)

            # Try to delete a shared object (should fail)
            pseudo_node = orm.load_node(pseudopotentials_created['Si']['pseudo_uuid'])
            repo_metadata = pseudo_node.backend_entity.repository_metadata or {}
            shared_key = list(dict(iter_repository_keys(repo_metadata)).values())[0]

            with pytest.raises(PermissionError, match='Cannot delete objects in shared repositories'):
                user1_repo.delete_objects([shared_key])
            print('  ✓ Deleting shared objects is correctly forbidden')

            # Verify object still exists after attempted delete
            assert user1_repo.has_objects([shared_key])[0], 'Shared object should still exist'
            print('  ✓ Shared object still exists after delete attempt')

            # ============================================================
            # PHASE 8: Summary
            # ============================================================
            print('\n' + '=' * 70)
            print('TEST SUMMARY')
            print('=' * 70)
            print(f'  user1 bucket ({buckets["user1"]}): {len(list_bucket_objects(s3_client, buckets["user1"]))} objects (should be 0)')
            print(f'  user2 bucket ({buckets["user2"]}): {len(list_bucket_objects(s3_client, buckets["user2"]))} objects (shared, read-only)')
            print(f'  Pseudopotentials tested: {list(pseudopotentials_created.keys())}')
            print(f'  All content verified via aiida API: ✓')
            print(f'  No data duplication: ✓')
            print(f'  Read-only enforcement: ✓')
            print('=' * 70)
            print('ALL TESTS PASSED!')
            print('=' * 70)

    def test_attach_shared_object_workflow(
        self,
        s3_test_config: dict[str, t.Any],
        config_psql_dos,
        aiida_config,
        aiida_profile_factory,
        tmp_path_factory,
    ):
        """Test attaching a shared object to a node in user1's profile without copying bytes."""
        from aiida import orm
        from aiida.manage import get_manager
        from aiida_s3.shared import attach_shared_object, iter_repository_keys
        from aiida_s3.repository.multi import make_shared_key

        buckets = s3_test_config['_buckets']
        s3_client = get_s3_client()
        tmp_path = tmp_path_factory.mktemp('attach_shared_test')

        shared_name = 'attach-test-shared'

        # Phase 1: Create data in user2's bucket
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
            name='user2-attach-test',
        ):
            # Create a pseudopotential in user2
            pseudo_content = make_pseudopotential_upf('Fe')
            pseudo_node = orm.SinglefileData(io.BytesIO(pseudo_content), filename='Fe.UPF')
            pseudo_node.store()
            pseudo_uuid = pseudo_node.uuid

            # Get the object key
            repo = get_manager().get_profile_storage().get_repository()
            keys = dict(iter_repository_keys(pseudo_node.backend_entity.repository_metadata or {}))
            object_key = list(keys.values())[0]
            print(f'Created Fe pseudopotential in user2, object key: {object_key}')

        # Phase 2: In user1, create a new node and attach the shared object
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
            name='user1-attach-test',
        ):
            # Create a new empty node in user1
            new_node = orm.Dict(dict={'description': 'Node with attached shared pseudopotential'})
            new_node.store()
            print(f'Created new node in user1: {new_node.uuid}')

            # Attach the shared object
            attach_shared_object(new_node, 'pseudos/Fe.UPF', shared_name, object_key)
            print(f'Attached shared object Fe.UPF to node')

            # Verify the content is readable
            content = new_node.base.repository.get_object_content('pseudos/Fe.UPF', mode='rb')
            assert content == pseudo_content, 'Content should match'
            print(f'✓ Content verified via aiida API: {len(content)} bytes')

            # Verify user1's bucket is still empty
            repo = get_manager().get_profile_storage().get_repository()
            user1_objects = list(repo.list_objects())
            print(f'user1 bucket objects: {len(user1_objects)}')
            assert len(user1_objects) == 0, 'user1 bucket should be empty - no bytes copied!'

            # Verify S3 directly
            s3_objects = list_bucket_objects(s3_client, buckets['user1'])
            assert len(s3_objects) == 0, f'S3 user1 bucket should be empty, has: {s3_objects}'
            print('✓ S3 user1 bucket confirmed empty - no data duplication!')

            # Verify the key is namespaced
            keys = dict(iter_repository_keys(new_node.backend_entity.repository_metadata or {}))
            attached_key = keys['pseudos/Fe.UPF']
            expected_key = make_shared_key(shared_name, object_key)
            assert attached_key == expected_key, f'Key should be {expected_key}, got {attached_key}'
            print(f'✓ Key correctly namespaced: {attached_key}')

            print('\nATTACH SHARED OBJECT TEST PASSED!')
