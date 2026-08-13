# Environment and Installation

## Prerequisites

- Docker and Docker Compose
- A Snowflake account, database, warehouse, and role able to create schemas
- An existing versioned S3 bucket for the raw archive
- AWS permission to create a scoped ingestion user and Snowflake reader role
- A Massive.com/Polygon.io API key when resuming source ingestion
- Python 3 for optional local commands

## Environment

Create the local environment file:

```bash
cp .env.example .env
```

Populate the provider, Snowflake, S3, stage, and private-key values documented
in `.env.example`. Historical `POLYGON_*` names and `api.polygon.io` remain
supported after the provider's Massive.com rebrand.

Store only the dedicated ingestion profile in these Git-ignored files:

```text
keys/aws-credentials
keys/aws-config
```

Docker Compose mounts them read-only at `/run/secrets`. Never commit AWS access
keys, Snowflake private keys, `.env`, or Streamlit secrets.

## AWS and Snowflake Integration

The checked-in infrastructure files document the current deployment and the
resources needed to recreate the S3 connection:

- `infra/aws/stock-market-s3-iam.yaml`
- `infra/snowflake/s3_raw_landing.sql`

For a new environment:

1. Create an S3 bucket, enable versioning, and choose the raw prefix.
2. Replace the account, role, and bucket values in the Snowflake SQL.
3. Create the Snowflake storage integration and run `DESC INTEGRATION
   STOCK_MARKET_S3_INTEGRATION` to obtain its IAM principal and external ID.
4. Deploy the CloudFormation IAM template using those trust values and the
   existing bucket name.
5. Set the bucket, prefix, profile, and external-stage values in `.env`.

The CloudFormation template intentionally assumes the S3 bucket already exists.
Do not redeploy it over manually created IAM resources in the existing portfolio
environment.

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
dbt seed --profiles-dir .
dbt parse --profiles-dir .
```

`dbt seed` loads the Russell 3000 constituent CSV snapshots into `SEEDS`.

## Streamlit

The hosted dashboard is available at:

<https://russell3000-market-intelligence.streamlit.app/>

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
