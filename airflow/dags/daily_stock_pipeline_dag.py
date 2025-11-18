# airflow/dags/daily_stock_pipeline_dag.py

from airflow.decorators import dag, task
from pendulum import timezone, datetime

# DAG for daily Polygon → Snowflake ingestion and dbt transformations.
@dag(
    dag_id="market_data_pipeline",
    schedule="0 12 * * 1-5", # Mon–Fri at noon ET
    start_date=datetime(2025, 8, 1, tz=timezone("America/New_York")),
    catchup=False,
    tags=["etl", "snowflake", "polygon", "dbt"],
)
def market_data_pipeline():
    """
    Daily ETL/ELT pipeline for Polygon → Snowflake → dbt.
    Steps:
      1) Extract + load grouped daily aggregates into RAW.DAILY_STOCKS
      2) Run dbt models (staging → intermediate → marts)
      3) Run dbt tests
    """
    @task()
    def extract():
        # src/ is on PYTHONPATH inside the container (mapped to /opt/airflow/)
        from src.extract_load_stocks import extract_load_data
        # For a daily schedule, only process the most recent date
        extract_load_data(days_back_override=1)

    # dbt is run layer-by-layer so failures surface at the correct stage
    @task.bash
    def run_dbt_staging():
        return """cd /opt/airflow/dbt/stock_analytics && \
            dbt run --select staging --profiles-dir .
        """

    @task.bash
    def run_dbt_intermediate():
        return """cd /opt/airflow/dbt/stock_analytics && \
            dbt run --select intermediate --profiles-dir .
        """

    @task.bash
    def run_dbt_marts():
        return """cd /opt/airflow/dbt/stock_analytics && \
            dbt run --select marts --profiles-dir .
        """

    @task.bash
    def run_dbt_tests():
        return """cd /opt/airflow/dbt/stock_analytics && \
            dbt test --profiles-dir .
        """
    
    # Enforce the ELT order: extract → dbt layers → dbt tests
    extract() >> run_dbt_staging() >> run_dbt_intermediate() >> run_dbt_marts() >> run_dbt_tests()

market_data_pipeline()