"""Verify state security and Terraform locking; never access the archive bucket.

Creates/deletes only its own temporary bootstrap .tflock object. Run with no other
bootstrap Terraform process active. Existing locks are never overwritten/removed.
Requires current boto3, the private bootstrap inputs, and initialized backends.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import uuid


ROOT = Path(__file__).resolve().parents[1]
BUCKET = 'stock-market-tfstate-645968351825-us-east-1'
ACCOUNT = '645968351825'


def main():
    import boto3
    from botocore.exceptions import ClientError
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--terraform', default='terraform')
    parser.add_argument('--variables', default='.cache/terraform/bootstrap.tfvars.json')
    args = parser.parse_args()
    binary = shutil.which(args.terraform)
    if binary is None:
        parser.error('Terraform is required')
    session = boto3.Session(profile_name=args.profile, region_name='us-east-1')
    identity = session.client('sts').get_caller_identity()
    if identity['Account'] != ACCOUNT:
        raise ValueError('Unexpected AWS account')
    s3 = session.client('s3')
    assert s3.get_bucket_versioning(Bucket=BUCKET)['Status'] == 'Enabled'
    encryption = s3.get_bucket_encryption(Bucket=BUCKET)['ServerSideEncryptionConfiguration']['Rules']
    assert all(rule['ApplyServerSideEncryptionByDefault']['SSEAlgorithm'] == 'AES256' for rule in encryption)
    assert all(s3.get_public_access_block(Bucket=BUCKET)['PublicAccessBlockConfiguration'].values())
    assert s3.get_bucket_policy_status(Bucket=BUCKET)['PolicyStatus']['IsPublic'] is False
    policy = json.loads(s3.get_bucket_policy(Bucket=BUCKET)['Policy'])
    statements = {p['Sid']: p for p in policy['Statement']}
    tls = statements['DenyInsecureTransport']
    restricted = statements['RestrictStateToAdministrator']
    assert tls['Effect'] == 'Deny' and tls['Condition']['Bool']['aws:SecureTransport'] == 'false'
    assert restricted['Effect'] == 'Deny'
    assert set(restricted['Condition']['ArnNotEquals']['aws:PrincipalArn']) == {
        identity['Arn'], f'arn:aws:iam::{ACCOUNT}:root'}
    for statement in [tls, restricted]:
        assert statement['Principal'] == '*' and statement['Action'] == 's3:*'
        assert set(statement['Resource']) == {f'arn:aws:s3:::{BUCKET}', f'arn:aws:s3:::{BUCKET}/*'}

    states = {}
    for root in ['bootstrap', 'aws', 'snowflake']:
        backend = json.loads((ROOT / 'infra/terraform' / root / '.terraform/terraform.tfstate').read_text())['backend']['config']
        assert backend['bucket'] == BUCKET and backend['key'] == f'{root}/terraform.tfstate'
        assert backend['use_lockfile'] is True and backend['encrypt'] is True
        metadata = s3.head_object(Bucket=BUCKET, Key=backend['key'])
        assert metadata['ServerSideEncryption'] == 'AES256' and metadata.get('VersionId')
        states[root] = 'encrypted_versioned_remote_state_verified'

    # A real ingestion identity must not list or read infrastructure state.
    ingestion = boto3.Session(profile_name='stock-market-ingestion', region_name='us-east-1')
    assert ingestion.client('sts').get_caller_identity()['Arn'] == f'arn:aws:iam::{ACCOUNT}:user/stock-market-ingestion'
    for operation, parameters in [
        ('list_objects_v2', {'Bucket':BUCKET, 'Prefix':'bootstrap/'}),
        ('head_object', {'Bucket':BUCKET, 'Key':'bootstrap/terraform.tfstate'}),
    ]:
        try:
            getattr(ingestion.client('s3'), operation)(**parameters)
        except ClientError as exc:
            assert exc.response['ResponseMetadata']['HTTPStatusCode'] == 403
        else:
            raise ValueError('Ingestion identity unexpectedly accessed state')

    lock_key = 'bootstrap/terraform.tfstate.tflock'
    lock_id = str(uuid.uuid4())
    lock = dict(ID=lock_id, Operation='OperationTypePlan', Info='Controlled locking verification',
                Who='stock-market-backend-check', Version='1.14.5',
                Created=datetime.now(timezone.utc).isoformat(), Path=BUCKET+'/bootstrap/terraform.tfstate')
    created = s3.put_object(Bucket=BUCKET, Key=lock_key, Body=json.dumps(lock).encode(),
                            IfNoneMatch='*', ServerSideEncryption='AES256')
    try:
        try:
            s3.put_object(Bucket=BUCKET, Key=lock_key, Body=b'{}', IfNoneMatch='*', ServerSideEncryption='AES256')
        except ClientError as exc:
            assert exc.response['ResponseMetadata']['HTTPStatusCode'] == 412
        else:
            raise ValueError('Conditional state locking failed')
        result = subprocess.run([binary, '-chdir='+str(ROOT/'infra/terraform/bootstrap'),
                                 'plan', '-refresh-only', '-input=false', '-no-color', '-lock-timeout=1s',
                                 '-var-file='+str((ROOT/args.variables).resolve())],
                                capture_output=True, text=True)
        assert result.returncode != 0 and 'Error acquiring the state lock' in result.stdout + result.stderr
    finally:
        current = json.loads(s3.get_object(Bucket=BUCKET, Key=lock_key)['Body'].read())
        if current.get('ID') != lock_id:
            raise ValueError('Lock ownership changed; refusing to delete another operation lock')
        s3.delete_object(Bucket=BUCKET, Key=lock_key, IfMatch=created['ETag'])
    try:
        s3.head_object(Bucket=BUCKET, Key=lock_key)
    except ClientError as exc:
        assert exc.response['ResponseMetadata']['HTTPStatusCode'] == 404
    else:
        raise ValueError('Verification lock was not released')
    print(json.dumps(dict(state_security='passed', remote_states=states,
                          ingestion_state_access='denied_as_expected', conditional_lock='passed',
                          terraform_lock_contention='passed', verification_lock_removed=True)))


if __name__ == '__main__':
    main()
