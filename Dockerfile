# ./Dockerfile

FROM apache/airflow:3.0.6-python3.12

USER airflow

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Ensure src/ is importable
ENV PYTHONPATH="/opt/airflow/src"

