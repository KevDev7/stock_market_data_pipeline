# Environment and Installation

## Prerequisites

- Docker and Docker Compose
- AWS and Snowflake accounts; provision infrastructure with Terraform for a new environment
- For normal runs, the existing Snowflake database/warehouse/role and versioned S3 archive
- A Polygon.io API key when resuming source ingestion
- Python 3 for optional local commands

## Environment

Create the local environment file:

```bash
cp .env.example .env
```

Set the provider, Snowflake, S3, stage, and private-key values documented
in `.env.example`. Historical `POLYGON_*` names and `api.polygon.io` remain
supported. Project-controlled provider names consistently use Polygon.
Existing installations using legacy reference names must complete the
[Polygon naming migration](polygon-naming-migration.md), then the
[dataset-folder migration](s3-layout-migration.md), then the
[date-only archive migration](date-archive-migration.md), before resuming ingestion.
The current archive folders are `daily-stock-prices/`, `ticker-catalog/`, and
`company-overview/`. Keep `AWS_S3_PREFIX=daily-stock-prices` in the private `.env`.
Compose forwards that same value to Airflow's configuration lookup so an older
saved Airflow variable cannot redirect writes to the retired price prefix.
Terraform provisions a scoped external stage for each folder; ingestion setup
checks them without modifying infrastructure.

Store only the dedicated ingestion profile in these Git-ignored files:

```text
keys/aws-credentials
keys/aws-config
```

Docker Compose mounts them read-only at `/run/secrets`. Never commit AWS access
keys, Snowflake private keys, `.env`, or Streamlit secrets.

## AWS and Snowflake Integration

The infrastructure setup is documented in the
[Terraform adoption and provisioning guide](../infra/terraform/README.md).
AWS and Snowflake have separate state; Airflow/Docker Compose/dbt remain in their
existing code-based configuration. Adoption does not require ingestion, a
warehouse rebuild, or backfilling data.

**Existing resources were safely adopted on 2026-10-19.** See the
[verification results](infrastructure-adoption-results.json). Do not create a
second CloudFormation manager for them or reimport them into a different state.

For another environment, follow the Terraform guide's state-backend bootstrap,
fresh provisioning and Snowflake/AWS trust sequence. Keep provider credentials
outside Git and use a dedicated infrastructure-administrator profile, not the
ingestion profile. Normal ingestion needs only the existing application credentials.

Terraform owns the database/RAW namespace, stages, file format, integration and
project infrastructure/access. The ingestion code owns raw/operational tables;
dbt owns transformed schemas/models. `infra/snowflake/s3_raw_landing.sql` now
contains application-owned landing and ingestion-metadata setup only. Historical AWS
templates/policies remain in `infra/legacy/` for provenance, not deployment.

All ingestion tracking lives in `MARKET.ADMIN`: daily price checkpoints,
reference load manifests, and historical price refresh manifests. Source payloads
remain in `MARKET.RAW`. For an existing checkout that still has manifests in RAW,
follow the [metadata migration procedure](operations.md#move-ingestion-manifests-to-admin)
before running updated ingestion setup. Setup refuses to create empty manifests
alongside unmigrated records. Fresh environments create the manifests in ADMIN.
The Python reference setup creates its own manifest; the explicit historical
refresh command creates its refresh manifest when that capability is first used.

Each endpoint has its own active RAW table: `DAILY_STOCKS_RAW`,
`TICKER_CATALOG_RAW`, and `COMPANY_OVERVIEW_RAW`. If the environment still uses
the shared `REFERENCE_RAW`, complete the [RAW table split](raw-reference-split.md)
before ingestion setup. Setup will not silently create empty replacements.
The retired shared table remains available only for recovery and historical
backup views; it receives no new loads.

## Snowflake Key Authentication

Generate an RSA key pair, assign the public key to the Snowflake user, and place
the private key in the Git-ignored `keys/` directory. `PRIVATE_KEY_PATH` must use
the path visible inside the Airflow container, for example:

```text
/opt/airflow/keys/rsa_key.pem
```

Both `src/snowflake_client.py` and the dbt profile use this key.

## Start Airflow

```bash
docker compose up -d
```

The Compose stack starts PostgreSQL for Airflow metadata plus the Airflow API
server, scheduler, and DAG processor. Open `http://localhost:8080` and confirm
that `market_data_pipeline` appears. DAGs are paused on creation.

## Initialize dbt

From a configured local environment or an Airflow container:

```bash
cd dbt/stock_analytics
dbt deps
dbt parse --profiles-dir .
```

Legacy Russell CSV seeds are disabled and retained only for provenance. Ingest
dated Polygon reference data with `python -m scripts.backfill_reference` from
the repository root; see the operations guide for validation and cutover.

## Streamlit

The hosted dashboard is available at:

<https://russell3000-market-intelligence.streamlit.app/>

The hosted app deploys from `main`, with `data-viz/streamlit_app.py` as its
entrypoint. Dashboard changes pushed to `main` update this deployment.
The [deployment migration record](hosted-dashboard-main-migration-results.json)
documents the branch switch and hosted verification on 2026-10-20.

For new Community Cloud deployments, select repository
`KevDev7/stock_market_data_pipeline`, branch `main`, and entrypoint
`data-viz/streamlit_app.py`. Use Python 3.12 and configure the Snowflake
credentials in Community Cloud's Secrets settings. Do not commit dashboard secrets.

For an existing app, check its configured branch in the Community Cloud workspace
before publishing changes or deleting branches. Changing the deployment branch
requires [redeploying the app](https://docs.streamlit.io/deploy/streamlit-community-cloud/manage-your-app/rename-your-app).
Preserve its secrets, Python version, sharing settings, and custom URL first.
Use Community Cloud's Reboot action if the running app has not picked up a commit
on its configured branch.

For local use, create `data-viz/.streamlit/secrets.toml`:

```toml
[snowflake]
account = "your_account"
user = "STREAMLIT_DASHBOARD_USER"
role = "STREAMLIT_DASHBOARD_ROLE"
warehouse = "STREAMLIT_DASHBOARD_WH"
database = "MARKET"
schema = "MARTS"
mart_schema = "MARTS"
private_key = """-----BEGIN PRIVATE KEY-----
...
-----END PRIVATE KEY-----"""
```

Then run:

```bash
pip install -r data-viz/requirements.txt
streamlit run data-viz/streamlit_app.py
```
