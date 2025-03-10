from datetime import datetime,timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import pendulum

# Directed Acyclic Graph 한글로 번역하면 방향성이 있는 비순환 그래프
with DAG(
        "seoul",
        schedule="@hourly",
        start_date=pendulum.datetime(2025, 3, 10 , tz="Asia/Seoul")
        
) as dag:
    
    Start = EmptyOperator(task_id="Start")
    End = EmptyOperator(task_id="End")
    
    b1 = BashOperator(task_id="b_1", 
                      bash_command="""
                      echo "date ======> 'date'"
                      echo "data_interval_start ======> {{data_interval_start}}"
                      echo "data_interval_end ======> {{data_interval_end}}"
                      echo "logical_date ======> {{logical_date}}"
                      echo "inlets ======> {{inlets}}"
                      echo "outlets ======> {{outlets}}"
                      echo "dag ======> {{dag}}"
                      echo "params ======> {{params}}"
                      echo "task_instance ======> {{task_instance}}"
                      """
                #       retries=5,
                #       retry_delay=timedelta(seconds=3),
                #       retry_exponential_backoff=True
                      )
    b2_1 = BashOperator(task_id="b_2_1", bash_command="echo 2_1")
    b2_2 = BashOperator(task_id="b_2_2", bash_command="echo 2_2")
    
#     Start >> b1 >> [b2_1 , b2_2] >> End 
#     Start >> b1
#     b1 >> b2_1
#     b1 >> b2_2
#     b2_1 >> End
#     b2_2 >> End
Start >> b1 >> b2_1
b1 >> b2_2
[b2_1 , b2_2] >> End