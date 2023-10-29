import os

import polars as pl

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
    'owner': 'natalia-zvezdina'
}

with DAG(
        dag_id="test-drive",
        schedule_interval="@once",
        default_args=default_args
) as dag:
    extract = BashOperator(
        task_id="download-extract-data",
        bash_command="curl -o /tmp/cell_towers.csv.xz https://datasets.clickhouse.com/cell_towers.csv.xz && \
                     xz -d /tmp/cell_towers.csv.xz"
    )

    def process_data():
        chunk_size = 100000
        os.makedirs("/tmp/batches", exist_ok=True)
        df = pl.scan_csv("/tmp/cell_towers.csv", has_header=True) \
            .filter(pl.col('mcc').is_in([262, 460, 310, 208, 510, 404, 250, 724, 234, 311])) \
            .collect()

        for i, chunk_start in enumerate(range(0, len(df), chunk_size)):
            chunk_end = chunk_start + chunk_size
            chunk = df[chunk_start:chunk_end]

            output_file_path = f"/tmp/batches/batch_{i + 1}.csv"
            chunk.write_csv(output_file_path, has_header=False)


    transform = PythonOperator(
        task_id="filter-formbatches",
        python_callable=process_data
    )

    extract