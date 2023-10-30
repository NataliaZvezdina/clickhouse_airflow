import csv
import logging
import os
from datetime import datetime

import polars as pl

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

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

        paths_list = []

        for i, chunk_start in enumerate(range(0, len(df), chunk_size)):
            chunk_end = chunk_start + chunk_size
            chunk = df[chunk_start:chunk_end]

            output_file_path = f"/tmp/batches/batch_{i + 1}.csv"
            chunk.write_csv(output_file_path, has_header=False)
            logging.info(f"Batch {i + 1} saved into: {output_file_path}")
            paths_list.append([output_file_path])
        return paths_list


    transform = PythonOperator(
        task_id="filter-formbatches",
        python_callable=process_data
    )

    def load_csv_to_clickhouse(fp):
        schema = {
            'radio': str,
            'mcc': int,
            'net': int,
            'area': int,
            'cell': int,
            'unit': int,
            'lon': float,
            'lat': float,
            'range': int,
            'samples': int,
            'changeable': int,
            'created': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'),
            'updated': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'),
            'averageSignal': int
        }
        bypass = lambda x: x

        cl_hook = ClickHouseHook(clickhouse_conn_id="clickhouse")

        fieldnames = ['radio', 'mcc', 'net', 'area', 'cell', 'unit', 'lon',
                      'lat', 'range', 'samples', 'changeable', 'created', 'updated', 'averageSignal']
        with open(fp) as f:
            gen = ({k: schema.get(k, bypass)(v) for k, v in row.items()} for row in
                   csv.DictReader(f, fieldnames=fieldnames))
            cl_hook.execute("insert into cell_towers values", gen)

    load = PythonOperator.partial(
        task_id="load-to-clickhouse",
        python_callable=load_csv_to_clickhouse
    ).expand(op_args=transform.output)

    extract >> transform >> load
