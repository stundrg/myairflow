from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pendulum

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='make_wiki',
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 2, tz='Asia/Seoul'),
    end_date=pendulum.datetime(2024, 2, 1, tz='Asia/Seoul'),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
    tags=['wiki', 'spark'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    end   = EmptyOperator(task_id='end')

    run_spark_job = BashOperator(
        task_id='run_spark_job',
        bash_command="""
        source ~/.bashrc    
        ssh -i ~/.ssh/stundrg wsl@34.64.90.247 "/home/wsl/code/test/run.sh {{ ds }} /home/wsl/code/test/make_wiki.py"
        """
    )
    start >> run_spark_job >> end