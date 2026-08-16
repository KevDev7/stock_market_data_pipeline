# airflow/dags/daily_stock_pipeline_dag.py

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pendulum import datetime, timezone

DBT_EXECUTABLE = "/home/airflow/.dbt-venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt/stock_analytics"


def run_raw_stock_ingestion():
    # src/ is on PYTHONPATH inside the container (mapped to /opt/airflow/)
    from src.extract_load_stocks import ingest_raw_stock_data

    # For a daily schedule, only process the most recent date
    ingest_raw_stock_data(days_back_override=1)


with DAG(
    dag_id="market_data_pipeline",
    schedule="0 12 * * 1-5",  # Mon-Fri at noon ET
    start_date=datetime(2025, 8, 1, tz=timezone("America/New_York")),
    catchup=False,
    tags=["elt", "s3", "snowflake", "polygon", "dbt"],
    doc_md="""
    Daily batch ELT pipeline for Polygon.io/Massive.com -> S3 -> Snowflake -> dbt.
    Steps:
      1) Extract + archive grouped daily aggregates in Amazon S3
      2) Load the archived object into RAW.DAILY_STOCKS_RAW
      3) Run dbt models (staging -> intermediate -> mart_staging -> marts)
      4) Run dbt tests
    """,
) as market_data_pipeline:

    ingest_raw_stock_data_task = PythonOperator(
        task_id="ingest_raw_stock_data",
        python_callable=run_raw_stock_ingestion,
    )

    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_EXECUTABLE} run --select staging --profiles-dir ."
        ),
    )

    run_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_EXECUTABLE} run --select intermediate --profiles-dir ."
        ),
    )

    run_dbt_mart_staging = BashOperator(
        task_id="run_dbt_mart_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_EXECUTABLE} run --select mart_staging --profiles-dir ."
        ),
    )

    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_EXECUTABLE} run --select marts --profiles-dir ."
        ),
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_EXECUTABLE} test --profiles-dir .",
    )

    # Enforce the ELT order: raw ingestion -> dbt layers -> dbt tests
    (
        ingest_raw_stock_data_task
        >> run_dbt_staging
        >> run_dbt_intermediate
        >> run_dbt_mart_staging
        >> run_dbt_marts
        >> run_dbt_tests
    )
