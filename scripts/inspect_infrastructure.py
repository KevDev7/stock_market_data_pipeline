"""Read-only inventory for Terraform adoption; no CREATE/ALTER/GRANT/data writes.

Run from repository root: python -m scripts.inspect_infrastructure snowflake
Optional --variables writes private, Git-ignored *.tfvars.json using mode 0600.
Snowflake uses the existing .env/key configuration; never prints credentials,
public keys, external IDs, email addresses, or unfiltered user descriptions.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def qualified(name):
    return '.'.join(quote(p) for p in name.split('.'))


def query(cursor, sql):
    cursor.execute(sql)
    columns = [c[0].lower() for c in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def properties(rows, key='property_value'):
    return {r['property']: r[key] for r in rows}


def snowflake_connection():
    from cryptography.hazmat.primitives import serialization
    import snowflake.connector
    from src.config import PROJECT_ROOT, SNOWFLAKE
    path = Path(SNOWFLAKE['private_key_path'])
    if not path.is_file():
        path = PROJECT_ROOT / 'keys' / path.name
    key = serialization.load_pem_private_key(path.read_bytes(), password=None)
    return snowflake.connector.connect(
        account=SNOWFLAKE['account'], user=SNOWFLAKE['user'], role=SNOWFLAKE['role'],
        warehouse=SNOWFLAKE['warehouse'], login_timeout=20,
        private_key=key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
                                     serialization.NoEncryption()),
        session_parameters={'QUERY_TAG': 'stock_market_iac_read_only_inventory'},
    )


def snowflake_inventory():
    """Inspect only known project objects; no account-wide ownership assumptions."""
    from src.config import AWS, SNOWFLAKE
    connection = snowflake_connection()
    try:
        cur = connection.cursor()
        database = SNOWFLAKE['database']
        db = quote(database)
        identity = query(cur, 'SELECT CURRENT_ORGANIZATION_NAME() AS ORGANIZATION, '
                         'CURRENT_ACCOUNT_NAME() AS ACCOUNT, CURRENT_USER() AS USER, CURRENT_ROLE() AS ROLE')[0]
        db_settings = next(r for r in query(cur, 'SHOW DATABASES') if r['name'] == database)
        raw_settings = next(r for r in query(cur, f'SHOW SCHEMAS IN DATABASE {db}') if r['name'] == 'RAW')
        db_retention = query(cur, f"SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN DATABASE {db}")[0]
        raw_retention = query(cur, f"SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN SCHEMA {db}.RAW")[0]
        integration = properties(query(cur, 'DESC INTEGRATION STOCK_MARKET_S3_INTEGRATION'))
        warehouse = next(r for r in query(cur, 'SHOW WAREHOUSES') if r['name'] == 'STREAMLIT_DASHBOARD_WH')
        role = next(r for r in query(cur, 'SHOW ROLES') if r['name'] == 'STREAMLIT_DASHBOARD_ROLE')
        user = properties(query(cur, 'DESC USER STREAMLIT_DASHBOARD_USER'), 'value')
        if user['TYPE'] != 'SERVICE':
            raise ValueError('Dashboard user is not a SERVICE user; do not change its type during adoption')
        if json.loads(user['DEFAULT_SECONDARY_ROLES']) != ['ALL']:
            raise ValueError('Secondary role configuration differs from the inspected ALL setting; review before adoption')
        grants = query(cur, 'SHOW GRANTS TO ROLE STREAMLIT_DASHBOARD_ROLE')
        future = query(cur, 'SHOW FUTURE GRANTS TO ROLE STREAMLIT_DASHBOARD_ROLE')
        memberships = query(cur, 'SHOW GRANTS TO USER STREAMLIT_DASHBOARD_USER')
        stages = query(cur, f'SHOW STAGES IN SCHEMA {db}.RAW')
        stage_settings = {
            name: [r for r in query(cur, f'DESC STAGE {db}.RAW.{quote(name)}')
                   if r['parent_property'] in ['STAGE_FILE_FORMAT', 'DIRECTORY']]
            for name in ['POLYGON_RAW_STAGE', 'POLYGON_TICKER_CATALOG_STAGE', 'POLYGON_COMPANY_OVERVIEW_STAGE']
        }
        counts = query(cur, f"SELECT TABLE_SCHEMA,TABLE_NAME,ROW_COUNT FROM {db}.INFORMATION_SCHEMA.TABLES "
                       "WHERE TABLE_SCHEMA IN ('RAW','MARTS') ORDER BY TABLE_SCHEMA,TABLE_NAME")
        published = {r['table_name'] for r in counts if r['table_schema'] == 'MARTS'
                     and r['table_name'].startswith(('DIM_', 'FCT_'))}
        selected_grants = {}
        for grant in grants:
            kind, name = grant['granted_on'], grant['name']
            scope = None
            if kind == 'DATABASE' and name == database:
                scope = 'account_object'
            elif kind == 'WAREHOUSE' and name == warehouse['name']:
                scope = 'account_object'
            elif kind == 'SCHEMA' and name == database + '.MARTS':
                scope = 'schema'
            elif kind == 'TABLE' and name in {database + '.MARTS.' + t for t in published}:
                scope = 'schema_object'
            if scope and grant['privilege'] != 'OWNERSHIP':
                key = re.sub('[^a-z0-9]+', '_', (kind+'_'+name+'_'+grant['privilege']).lower())
                selected_grants[key] = dict(role=role['name'], privileges=[grant['privilege']], scope=scope,
                                           object_type=kind, object_name=qualified(name),
                                           grant_option=str(grant['grant_option']).lower() == 'true')
        for grant in future:
            if grant['name'].startswith(database + '.MARTS.'):
                key = 'future_' + grant['grant_on'].lower() + '_' + grant['privilege'].lower()
                selected_grants[key] = dict(role=role['name'], privileges=[grant['privilege']], scope='future',
                                           object_type=grant['grant_on']+'S', object_name=db+'."MARTS"',
                                           grant_option=str(grant['grant_option']).lower() == 'true')
        variables = dict(
            organization_name=identity['organization'], account_name=identity['account'], admin_role=identity['role'],
            database_name=database, database_comment=db_settings['comment'],
            retention_days=int(db_retention['value']) if db_retention['level'] == 'DATABASE' else None,
            raw_retention_days=int(raw_retention['value']) if raw_retention['level'] == 'SCHEMA' else None,
            raw_schema_name='RAW', raw_schema_comment=raw_settings['comment'], archive_bucket=AWS['s3_bucket'],
            price_prefix=AWS['s3_prefix'], reference_prefix='ticker-catalog',
            overview_prefix='company-overview', adopt_overview_stage=True,
            storage_role_arn=integration['STORAGE_AWS_ROLE_ARN'], storage_integration_name='STOCK_MARKET_S3_INTEGRATION',
            storage_integration_comment=integration['COMMENT'], storage_external_id=integration['STORAGE_AWS_EXTERNAL_ID'],
            price_stage_name='POLYGON_RAW_STAGE', reference_stage_name='POLYGON_TICKER_CATALOG_STAGE',
            overview_stage_name='POLYGON_COMPANY_OVERVIEW_STAGE',
            file_format_name='POLYGON_NDJSON_GZIP',
            warehouses={'dashboard': dict(name=warehouse['name'], size=warehouse['size'].upper().replace('X-SMALL','XSMALL'),
                auto_suspend=warehouse['auto_suspend'], auto_resume=warehouse['auto_resume'] == 'true',
                generation=warehouse['generation'], resource_monitor=warehouse['resource_monitor'],
                enable_query_acceleration=warehouse['enable_query_acceleration'] == 'true',
                query_acceleration_max_scale_factor=warehouse['query_acceleration_max_scale_factor'], comment=warehouse['comment'])},
            roles={'dashboard': dict(name=role['name'], comment=role['comment'])},
            service_users={'dashboard': dict(name=user['NAME'], login_name=user['LOGIN_NAME'], display_name=user['DISPLAY_NAME'],
                disabled=user['DISABLED'], default_role=user['DEFAULT_ROLE'], default_warehouse=user['DEFAULT_WAREHOUSE'],
                default_namespace=user['DEFAULT_NAMESPACE'], default_secondary_roles_option='ALL',
                comment=None if user.get('COMMENT') in [None,'null',''] else user['COMMENT'])},
            role_memberships={r['role']: dict(role=r['role'], user=r['grantee_name']) for r in memberships
                              if r['role'] == role['name']},
            grants=selected_grants,
        )
        report = dict(identity=identity, database={'name': database, 'owner': db_settings['owner']},
                      dedicated_warehouse=variables['warehouses'], roles=variables['roles'],
                      service_user_type=user['TYPE'], managed_grants=selected_grants,
                      retained_unmanaged_backup_grants=sum('BACKUP_' in g['name'] for g in grants),
                      stage_settings=stage_settings,
                      stages=[{k: r[k] for k in ['name','url','storage_integration','owner']} for r in stages],
                      integration={k: integration[k] for k in ['ENABLED','STORAGE_ALLOWED_LOCATIONS','STORAGE_AWS_ROLE_ARN']},
                      metadata_row_counts=counts,
                      referenced_not_managed=['COMPUTE_WH','AIRFLOW_USER','ACCOUNTADMIN','SYSADMIN',warehouse['resource_monitor']])
        return report, variables
    finally:
        connection.close()


def aws_inventory(profile):
    """SDK fallback when the connected AWS tool cannot use a selected local profile."""
    import boto3
    session = boto3.Session(profile_name=profile, region_name='us-east-1')
    sts, s3, iam = (session.client(service) for service in ['sts','s3','iam'])
    rule_shape = s3.meta.service_model.operation_model('GetBucketEncryption').output_shape.members[
        'ServerSideEncryptionConfiguration'].members['Rules'].member
    if 'BlockedEncryptionTypes' not in rule_shape.members:
        raise ValueError('Upgrade boto3/botocore before inventory: this SDK would omit existing encryption restrictions')
    identity = sts.get_caller_identity()
    if identity['Account'] != '645968351825':
        raise ValueError('AWS profile is not in the expected project account')
    bucket = 'stock-market-data-pipeline-645968351825-us-east-1'
    # Complete IAM discovery must succeed before generating adoption variables.
    user = iam.get_user(UserName='stock-market-ingestion')['User']
    role = iam.get_role(RoleName='snowflake-stock-market-raw-reader')['Role']
    def all_items(operation, result_key, **kwargs):
        return [item for page in iam.get_paginator(operation).paginate(**kwargs) for item in page[result_key]]
    user_inline = {name: json.dumps(iam.get_user_policy(UserName=user['UserName'], PolicyName=name)['PolicyDocument'])
                   for name in all_items('list_user_policies','PolicyNames',UserName=user['UserName'])}
    role_inline = {name: json.dumps(iam.get_role_policy(RoleName=role['RoleName'], PolicyName=name)['PolicyDocument'])
                   for name in all_items('list_role_policies','PolicyNames',RoleName=role['RoleName'])}
    user_attached = all_items('list_attached_user_policies','AttachedPolicies',UserName=user['UserName'])
    role_attached = all_items('list_attached_role_policies','AttachedPolicies',RoleName=role['RoleName'])
    # Do not infer ownership of attached managed policies from their names alone.
    attached = {p['PolicyArn'] for p in user_attached + role_attached}
    if attached:
        raise ValueError('Attached managed policies require ownership review before generating variables; inspect IAM attachments')
    try:
        bucket_tags = {t['Key']: t['Value'] for t in s3.get_bucket_tagging(Bucket=bucket)['TagSet']}
    except s3.exceptions.ClientError as exc:
        if exc.response['Error']['Code'] != 'NoSuchTagSet':
            raise
        bucket_tags = {}
    rules = s3.get_bucket_encryption(Bucket=bucket)['ServerSideEncryptionConfiguration']['Rules']
    if len(rules) != 1 or rules[0]['ApplyServerSideEncryptionByDefault']['SSEAlgorithm'] != 'AES256':
        raise ValueError('Archive encryption differs from the inspected AES256 configuration; review before adoption')
    rule = rules[0]
    tags = lambda rows: {t['Key']: t['Value'] for t in rows}
    variables = dict(aws_profile=profile, account_id=identity['Account'], region='us-east-1', archive_bucket=bucket,
        bucket_tags=bucket_tags, versioning_status=s3.get_bucket_versioning(Bucket=bucket).get('Status','Disabled'),
        bucket_key_enabled=rule.get('BucketKeyEnabled',False),
        blocked_encryption_types=rule.get('BlockedEncryptionTypes',{}).get('EncryptionType',[]),
        public_access={
            'block_public_acls': False, 'block_public_policy': False,
            'ignore_public_acls': False, 'restrict_public_buckets': False},
        ingestion_user=dict(name=user['UserName'], path=user['Path'], tags=tags(user.get('Tags',[])),
            permissions_boundary=user.get('PermissionsBoundary',{}).get('PermissionsBoundaryArn')),
        reader_role=dict(name=role['RoleName'], path=role['Path'], description=role.get('Description',''),
            max_session_duration=role['MaxSessionDuration'], tags=tags(role.get('Tags',[])),
            permissions_boundary=role.get('PermissionsBoundary',{}).get('PermissionsBoundaryArn'),
            assume_role_policy=json.dumps(role['AssumeRolePolicyDocument'])),
        user_inline_policies=user_inline, role_inline_policies=role_inline)
    block = s3.get_public_access_block(Bucket=bucket)['PublicAccessBlockConfiguration']
    variables['public_access'] = dict(zip(variables['public_access'], [block[k] for k in
        ['BlockPublicAcls','BlockPublicPolicy','IgnorePublicAcls','RestrictPublicBuckets']]))
    report = dict(identity={k: identity[k] for k in ['Account','Arn']},
                  bucket=bucket, bucket_versioning=variables['versioning_status'],
                  bucket_tags=bucket_tags, public_access=variables['public_access'],
                  encryption={'algorithm':'AES256', 'bucket_key_enabled':variables['bucket_key_enabled'],
                              'blocked_encryption_types':variables['blocked_encryption_types']},
                  ingestion_user=user['UserName'], reader_role=role['RoleName'],
                  user_inline_policy_names=sorted(user_inline), role_inline_policy_names=sorted(role_inline))
    return report, variables


def write_private_variables(path, variables):
    if not str(path).endswith('.tfvars.json'):
        raise ValueError('Private variable output must end in .tfvars.json (Git-ignored)')
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w') as handle:
        json.dump(variables, handle, indent=2, default=str)
        handle.write('\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('service', choices=['aws','snowflake'])
    parser.add_argument('--profile', default='default')
    parser.add_argument('--variables', help='Optional private *.tfvars.json output; refuses to overwrite existing files')
    args = parser.parse_args()
    report, variables = aws_inventory(args.profile) if args.service == 'aws' else snowflake_inventory()
    if args.variables:
        write_private_variables(args.variables, variables)
    print(json.dumps(dict(observed_at=datetime.now(timezone.utc).isoformat(), service=args.service,
                          read_only=True, inventory=report), indent=2, default=str))


if __name__ == '__main__':
    main()
