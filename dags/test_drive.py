from airflow import DAG
from airflow.operators.bash import BashOperator
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

    extract