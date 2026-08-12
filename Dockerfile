# ./Dockerfile

FROM apache/airflow:3.0.6-python3.12

USER airflow

# Keep Airflow and dbt dependencies isolated because their transitive version
# constraints are incompatible in a single Python environment.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    pip check

COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /home/airflow/.dbt-venv && \
    /home/airflow/.dbt-venv/bin/pip install --no-cache-dir \
        -r /tmp/requirements-dbt.txt && \
    /home/airflow/.dbt-venv/bin/pip check

# Ensure src/ is importable
ENV PYTHONPATH="/opt/airflow/src"
