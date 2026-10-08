# airflow/dags/daily_stock_pipeline_dag.py

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pendulum import datetime, timezone
import datetime as dt
import json
import os
import shlex

DBT_EXECUTABLE = "/home/airflow/.dbt-venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt/stock_analytics"
STUDY_VARS={
    'analysis_start':os.getenv('ANALYSIS_START','2024-01-01'),
    'analysis_end':os.getenv('ANALYSIS_END','2025-12-31'),
    'warmup_start':os.getenv('WARMUP_START','2022-12-29'),
}
for study_date in STUDY_VARS.values():
    dt.date.fromisoformat(study_date)
DBT_VARS_ARG=shlex.quote(json.dumps(STUDY_VARS))


def extract_stock_data():
    # src/ is on PYTHONPATH inside the container (mapped to /opt/airflow/)
    from src.extract_load_stocks import extract_load_data

    # For a daily schedule, only process the most recent date
    extract_load_data(days_back_override=1)


def extract_reference_data():
    from scripts.backfill_reference import backfill
    from src.extract_load_stocks import latest_completed_trading_day
    day=str(latest_completed_trading_day())
    backfill(start_date=day,end_date=day,
             analysis_start=STUDY_VARS['analysis_start'],analysis_end=STUDY_VARS['analysis_end'],
             cache_path='/opt/airflow/logs/.cache/massive-reference.sqlite3')


with DAG(
    dag_id="market_data_pipeline",
    schedule="0 12 * * 1-5",  # Mon-Fri at noon ET
    start_date=datetime(2025, 8, 1, tz=timezone("America/New_York")),
    catchup=False,
    # Snowflake does not enforce unique raw keys; serialize source-date replacements.
    max_active_runs=1,
    tags=["elt", "s3", "snowflake", "polygon", "dbt"],
    doc_md="""
    Daily batch ELT pipeline for Polygon.io/Massive.com -> S3 -> Snowflake -> dbt.
    Steps:
      1) Extract + archive grouped daily aggregates in Amazon S3
      2) Load the archived object into RAW.DAILY_STOCKS_RAW
      3) Archive/load the matching daily catalog and required issuer observations
      4) Run dbt models (staging -> intermediate -> marts)
      5) Run dbt tests. Default analytics remain the fixed 2024–2025 study.
    """,
) as market_data_pipeline:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_stock_data,
    )

    reference = PythonOperator(
        task_id="extract_reference",
        python_callable=extract_reference_data,
    )

    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_EXECUTABLE} run --select staging --profiles-dir . --vars {DBT_VARS_ARG}"
        ),
    )

    run_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_EXECUTABLE} run --select intermediate --profiles-dir . --vars {DBT_VARS_ARG}"
        ),
    )

    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_EXECUTABLE} run --select marts --profiles-dir . --vars {DBT_VARS_ARG}"
        ),
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_EXECUTABLE} test --profiles-dir . --vars {DBT_VARS_ARG}",
    )

    # Enforce the ELT order: extract -> dbt layers -> dbt tests
    (
        extract
        >> reference
        >> run_dbt_staging
        >> run_dbt_intermediate
        >> run_dbt_marts
        >> run_dbt_tests
    )
