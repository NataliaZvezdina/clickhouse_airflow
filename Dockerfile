FROM apache/airflow:2.5.3-python3.8
USER root
RUN apt-get update && apt-get install -y xz-utils
USER airflow
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user airflow-clickhouse-plugin polars