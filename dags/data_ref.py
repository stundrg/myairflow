from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='data_ref',
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 2),
    end_date=pendulum.datetime(2024, 2, 1),
    schedule_interval='10 10 * * *',
    catchup=True,
    max_active_runs=1,
    tags=['wiki', 'spark', 'data_ref'],
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')


    wiki_traffic_compare = BashOperator(
        task_id='wiki_traffic_compare',
        bash_command='''
        ssh -i ~/.ssh/stundrg wsl@34.64.90.247 "/home/wsl/code/test/run.sh {{ ds_nodash }} /home/wsl/code/test/wiki_traffic_compare.py"
        ''',
    )

    start >>  wiki_traffic_compare >> end