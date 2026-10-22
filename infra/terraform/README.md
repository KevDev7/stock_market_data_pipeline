# Infrastructure ownership and adoption

Terraform owns the project's adopted AWS infrastructure and Snowflake infrastructure/access.
It does **not** own pipeline data, dbt models, Airflow, or Docker Compose.

## Current rollout status

Completed on 2026-10-19 using the existing authorized `synthea-cdc` AWS profile.
The new private state backend was created, bootstrap state was migrated into it,
and **8 existing AWS resources + 23 existing Snowflake resources were imported**.
The initial import-only rollout adopted 22 Snowflake resources; the dashboard
service user was subsequently adopted using the narrowly audited metadata-triggered
exception described below. Its provider update issued a redundant lock-timer UNSET;
the login was already unlocked and remained unlocked. Existing credentials and
effective settings were preserved; no resources were replaced or deleted.
All three roots passed subsequent no-change plans. The loader now checks
Terraform-owned prerequisites rather than altering integrations or creating stages.

The `default` profile and application credential files were not changed. Never
use the administrator profile as the Airflow ingestion profile. See the
[verification results](../../docs/infrastructure-adoption-results.json) for
state security/locking checks, reader access, unchanged metadata row counts,
and the completed dashboard-user adoption audit below.

This checkout's current private inputs are `.cache/terraform/bootstrap.tfvars.json`,
`.cache/terraform/aws-live.tfvars.json`, and `.cache/terraform/snowflake-live.tfvars.json`;
matching `*.backend.hcl` files select the three state keys. Use those current
inputs for maintenance plans. The discovery commands below illustrate a new
adoption; choose a new output filename when refreshing inventory, because the
helper deliberately refuses to overwrite an existing snapshot.

## Ownership boundaries

| Owner | Objects |
|---|---|
| `bootstrap` Terraform root | Separate private state bucket, encryption, versioning, public-access block, administrator-only/TLS policy |
| `aws` Terraform root | Archive bucket settings, dedicated ingestion IAM user, reader IAM role, project policies/attachments |
| `snowflake` Terraform root | MARKET database, RAW namespace, S3 integration, three external stages, JSON file format, dedicated dashboard warehouse/role/service user, explicit active-mart grants and user-role membership |
| Ingestion setup | Raw tables, checkpoints, manifests, ADMIN namespace; temporary landing tables |
| dbt and cutover scripts | Transformed schemas, tables, views; grant mirroring during schema swaps |
| Existing account setup | Shared COMPUTE_WH, AIRFLOW_USER, administrative roles, account-wide PUBLIC grants, existing resource monitor |

Backup schemas/grants remain untouched. A role's explicitly managed grants are
not an exclusive list of everything it may possess. Do not replace them with
account-wide or `all_privileges` grants. The existing dashboard resource monitor
is referenced without changing its budget or triggers; provision an equivalent
monitor explicitly when setting up another account.

The later [dataset-folder migration](../../docs/s3-layout-migration.md) preserves
the price stage, renames the shared reference stage in place to
`POLYGON_TICKER_CATALOG_STAGE`, and creates `POLYGON_COMPANY_OVERVIEW_STAGE`.
There are now 24 managed Snowflake resources: 23 adopted resources plus that one
new stage. Final integration and IAM access is limited to the three dataset
prefixes, not the bucket root. The migration-only `manage_overview_stage=false`
switch leaves the old two-stage environment intact during preparation; keep it
true after cutover. `adopt_overview_stage` is false for initial creation and true
only when importing an already existing overview stage into another state.

The current private Snowflake inputs explicitly set `manage_service_users=true`.
The variable remains default-off for other deployments. Terraform now manages
`STREAMLIT_DASHBOARD_USER` as well as its existing role membership; authentication
key registration remains separate and existing keys are preserved.

Provider 2.22.0 leaves `mins_to_unlock` unset during user import but compares it
with its internal `-1` default when computing differences. Even with the timer
excluded from reconciliation, that comparison marks computed `show_output`
unknown and proposes an update with no configurable-field differences. The
confirmed trigger is in the provider's [user import/read implementation](https://github.com/snowflakedb/terraform-provider-snowflake/blob/v2.22.0/pkg/resources/user.go)
and [computed-output diff helper](https://github.com/snowflakedb/terraform-provider-snowflake/blob/v2.22.0/pkg/resources/custom_diffs.go).
This finding does not establish that any particular upstream issue is resolved.

One reviewed saved plan adopted this user with that metadata-triggered update. It
did not ignore `show_output`, edit state manually, or change credentials. Subsequent
plans must use the ordinary strict checker with `--no-imports`, without the
exception. Do not re-enable the exception to conceal a persistent difference.

## Requirements and secrets

- Terraform **1.14.5**; provider versions are pinned and their lockfiles are included.
- An existing administrator AWS profile for the expected account; Snowflake
  authentication capable of reading/managing the project objects.
- Python 3.11+ and current dependencies from `requirements.txt` for the helpers.
  AWS inventory refuses SDKs that would silently omit encryption restrictions;
  upgrade boto3/botocore in the isolated infrastructure environment if it reports
  that error. Do not change the running application's credentials or profile.
- Keep `.tfvars.json`, backend files, state, plans, and provider logs outside Git.
  Use `.cache/terraform/` for private inputs and `umask 077` before commands.
- Supply Snowflake authentication via `SNOWFLAKE_USER`,
  `SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT`, and `SNOWFLAKE_PRIVATE_KEY` in the
  process environment (or a private provider profile). Never put private keys in
  HCL, tfvars, CLI arguments, or outputs. The preview helper loads the existing
  saved key in memory. Supplying credentials to Terraform does not rotate them.
- Public-key registration and AWS access-key issuance remain separate manual
  operations. Existing keys are preserved, not recreated. Terraform state can
  still contain sensitive provider metadata: treat it as a secret even though
  the configured credentials are not resource attributes.

The provider's generated external ID, imported public keys, email address, and
temporary unlock timer are intentionally not reconciled. These are the narrow
`ignore_changes` exceptions; bucket settings, permissions, URLs, and model
ownership are not ignored to manufacture a clean plan.

## 1. Read-only discovery

Prefer the connected AWS tool for discovery. If it cannot select the authorized
local profile, the SDK helper is a fallback (it never uses the AWS CLI):

```bash
python -m scripts.inspect_infrastructure aws --profile EXISTING_ADMIN_PROFILE --variables .cache/terraform/aws.tfvars.json
python -m scripts.inspect_infrastructure snowflake --variables .cache/terraform/snowflake.tfvars.json
```

The helper refuses incomplete IAM discovery and refuses to infer ownership of
attached managed policies. If attachments exist, inspect their policy versions,
tags and other consumers first: reference shared policies; populate
`managed_policies` only for verified project-owned policies. Preserve actual
policy names/documents and attachments. Never fill missing inventory with the
old CloudFormation template's assumed names or defaults.

The Snowflake helper includes only project grants and preserves inherited
retention rather than converting it to an explicit parameter assignment.
It prints a sanitized report and optionally writes private inputs with mode
0600; existing files are never overwritten. Save reports privately and commit
only the reviewed, sanitized verification summary.

## 2. Bootstrap the state backend

This is the sole intentionally new infrastructure. Review the five-resource
bootstrap plan separately from workload adoption; the import-only guard is not
for this new-resource plan.

1. Copy `bootstrap/terraform.tfvars.example` to a Git-ignored tfvars file and
   replace the administrator identity/profile. Verify that identity first.
   A role must use its **IAM role ARN**, not an assumed-role session ARN.
2. Confirm the proposed state-bucket name is available. If a state bucket already
   exists, inspect/import it rather than trying to replace it.
3. Initialize/bootstrap locally, save and review a plan, then apply that exact
   plan. It must create only this project's state bucket and four settings/policy
   resources, never archive/IAM/Snowflake objects.
4. Copy `bootstrap/backend.tf.example` to `bootstrap/backend.tf` (Git-ignored).
   Create three private backend files from `backend.hcl.example`, with keys
   `bootstrap/terraform.tfstate`, `aws/terraform.tfstate`, and
   `snowflake/terraform.tfstate`. Use the same authorized profile and bucket.
5. Run `terraform -chdir=infra/terraform/bootstrap init -migrate-state
   -backend-config=PATH_TO_BOOTSTRAP_BACKEND_FILE` to migrate local bootstrap
   state. Keep a private recovery copy until remote state/versioning is verified.
6. After workload adoption, verify security and real lock contention with
   `python -m scripts.verify_terraform_backend --profile EXISTING_ADMIN_PROFILE`.
   This verifies the three encrypted/versioned state objects, confirms that the
   ingestion identity cannot list/read them, and creates only its own conditional
   bootstrap lockfile. Terraform must reject a second lock attempt. The helper
   removes only its own lock; do not force-unlock an active operation.

The bucket policy **denies** other principals but does not issue administrator
credentials or grant underlying IAM permissions. Backend access needs bucket
listing, state GetObject/PutObject, and lockfile GetObject/PutObject/DeleteObject.
Archive data-reader/ingestion identities must not gain state access.

## 3. Adopt existing workloads without changing them

`adopt_existing=true` is the default. Do not turn it off for this deployment.
Initialize each root with its own private backend file. Run commands below from
the repository root, first for `aws`, then substitute `snowflake`:

```bash
terraform -chdir=infra/terraform/aws init -backend-config=PATH_TO_AWS_BACKEND_FILE
terraform -chdir=infra/terraform/aws plan -input=false -var-file=../../../.cache/terraform/aws.tfvars.json -out=../../../.cache/terraform/aws.tfplan
terraform -chdir=infra/terraform/aws show -json ../../../.cache/terraform/aws.tfplan | python -m scripts.check_terraform_adoption --require-imports
```

Review the resource inventory, grant coverage and plan as well as the guard's
summary. **Stop on any creation, update, deletion, replacement, drift, incomplete
plan, deferred operation, or unresolved check.** Do not disable protections,
transfer ownership, use `-target`, or add broad `ignore_changes` to bypass a
problem. Unsupported imports stay unmanaged and must be documented.

Normally only an import-only plan may be applied, using the **same saved binary plan**:

```bash
terraform -chdir=infra/terraform/aws apply ../../../.cache/terraform/aws.tfplan
```

Then make a fresh saved plan, pass it through the guard with `--no-imports`, and
require no resource changes. Repeat for Snowflake. Import blocks are idempotent
and stay checked in; they do not reimport objects already in state.

### Narrow dashboard-user import exception (completed)

The explicit checker option `--allow-dashboard-user-metadata-refresh` permits
only the exact `snowflake_service_user.project["dashboard"]` import ID
`"STREAMLIT_DASHBOARD_USER"`, one in-place update, and a computed `show_output`
refresh. Every other before/after field must match, no other field may be unknown,
and no replacement, other resource mutation, drift, or extra import is permitted.
The summary reports the planned update separately; it is not an import-only plan.

For this completed adoption, dashboard authentication, all eight published marts,
settings, public-key fingerprints, ownership, role membership, user parameters,
saved credential-file hashes, and RAW/MARTS metadata row counts were compared
before and after. The exact reviewed binary plan was applied under remote backend
locking. Final plans require 23 Snowflake resources with no changes or imports.
The option is default-off and is not needed for ongoing maintenance or fresh
provisioning. Future updates outside this exact exception need a separately
reviewed plan; do not expand the allowlist to bypass a failure.

**Observed execution caveat:** a metadata-only plan difference does not prove that
the provider emits no SQL. Query-history verification caught one successful
`ALTER USER "STREAMLIT_DASHBOARD_USER" UNSET MINS_TO_UNLOCK`. The provider's
[update helper](https://github.com/snowflakedb/terraform-provider-snowflake/blob/v2.22.0/pkg/resources/resource_helpers_update.go)
treats its internal timer default as an UNSET. The pre-import output showed an
unlocked user with no lock expiry; post-checks confirmed that state was unchanged,
the timer was unset, and credentials/settings/grants still matched. No other
user-altering statements were observed. This was explicitly investigated after
the initial no-user-SQL check failed, not silently treated as a metadata-only
execution. No corrective user-setting apply or credential operation was performed.

Dashboard authentication tests update the computed `last_success_login` timestamp.
That is observable metadata drift, not a setting change. After all reader tests,
the new timestamp was recorded through a separately audited native Terraform
refresh-only plan: only `show_output.last_success_login` differed and there were
zero resource actions. Final ordinary plans then passed the strict no-drift guard.
Do not broaden the guard or edit state manually to conceal such observations;
inspect and record them through the same state-only review procedure when needed.

For a separate pre-adoption investigation, a disposable Snowflake preview is available:

```bash
python -m scripts.preview_snowflake_adoption --variables .cache/terraform/snowflake.tfvars.json
```

It strips the backend only in a temporary copy, plans existing-resource imports,
and destroys its local scratch files on exit. It **never applies or establishes
persistent Terraform ownership**. This is not a substitute for remote adoption.

## 4. Application ownership and post-adoption verification

The ownership cutover was made only after both import-only applies and clean
follow-up plans. `ReferenceWarehouse.check_infrastructure()` checks the enabled
S3 integration, allowed location, stage URL/integration and JSON/AUTO format
using DESC statements only. Setup then retains application-owned reference tables
and manifests. Missing prerequisites fail with Terraform setup guidance rather
than silently mutating infrastructure.

`SnowflakeClient.ensure_objects_exist()` inspects the configured RAW namespace
instead of recreating it; ADMIN/checkpoint/table setup remains application-owned.
`infra/snowflake/s3_raw_landing.sql` contains only application landing/checkpoint
setup. The old CloudFormation/policy files are archived in `infra/legacy/`.

Regression tests cover valid, missing, disabled and misconfigured prerequisites,
table setup, absence of integration/stage mutations, and existing loading behavior.
Cloud verification reads archive samples and published marts, compares metadata
row counts and checks no-change plans. None of this requires ingestion/backfill
or a dbt build. The adoption guard is for adoption/no-change verification; future
intentional infrastructure changes require their own reviewed plans.

## Fresh environment (not the current deployment)

Use different resource names/accounts/backends, `adopt_existing=false`, and
review creation plans explicitly rather than using the import-only guard.

1. Bootstrap state, supply existing administrative credentials, and arrange the
   referenced shared warehouse/resource monitor (or select existing equivalents).
2. With `manage_service_users=true`, provision the Snowflake database/RAW namespace/integration/stages and dedicated
   role/user/warehouse, initially with `grants={}` and `role_memberships={}`.
   The S3 integration may reference the intended AWS role ARN before trust is
   completed. Preserve its generated external ID and IAM principal privately.
3. Provision the AWS bucket/user/reader role and exact project-scoped policies,
   using those Snowflake trust values. No data copying is involved.
4. Register public keys/issue ingestion credentials separately and supply the
   existing `.env`/secret mounts. Run landing-table setup and the normal first
   pipeline/dbt initialization when data is intentionally required.
5. Apply the explicit dashboard grants/membership after the dbt-owned MARTS
   schema and published tables exist. Do not provision these tables with Terraform.

Fresh provisioning is documented but is not a claim that a second cloud
environment was created/tested during adoption work.

## Verification and recovery

```bash
terraform fmt -check -recursive infra/terraform
terraform -chdir=infra/terraform/bootstrap init -backend=false
terraform -chdir=infra/terraform/bootstrap validate
terraform -chdir=infra/terraform/bootstrap test
terraform -chdir=infra/terraform/aws init -backend=false
terraform -chdir=infra/terraform/aws validate
terraform -chdir=infra/terraform/snowflake init -backend=false
terraform -chdir=infra/terraform/snowflake validate
python -m unittest discover -s tests -p test_terraform_adoption.py
```

Mock tests and validation do not establish cloud access, successful locking, or
successful adoption. In particular `prevent_destroy` is only a guard while the
resource declaration remains in configuration; it is not a backup mechanism.

For targeted cloud-read checks (Python 3.11+, with existing local credentials):

```bash
python -m scripts.verify_infrastructure_access
```

This reads one retained record through each stage, checks the eight marts using
the saved dashboard-reader credentials, and reports metadata row counts. It may
briefly resume existing warehouses. It does not change infrastructure or data.

Recover state from a known S3 version under administrator control, then refresh
and review a plan before making changes. Do not restore state while another
Terraform operation holds a lock. To relinquish Terraform ownership, remove the
object from state using the documented state-removal workflow without destroying
the remote object; remove/disable its matching import block as part of that
review. Never use `terraform destroy` as rollback. A state/import problem does
not justify deleting archives or rebuilding warehouse data.

Official references: [Terraform import](https://developer.hashicorp.com/terraform/language/import),
[S3 backend and locking](https://developer.hashicorp.com/terraform/language/backend/s3),
[Snowflake provider](https://registry.terraform.io/providers/snowflakedb/snowflake/2.22.0/docs).
