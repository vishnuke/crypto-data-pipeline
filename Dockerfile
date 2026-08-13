FROM apache/airflow:2.9.3-python3.11

# Keep the image lean - only what the DAGs actually need
COPY requirements.txt /requirements.txt

USER airflow
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"