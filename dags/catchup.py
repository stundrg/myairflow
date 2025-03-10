from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
import pendulum

# Directed Acyclic Graph 한글로 번역하면 방향성이 있는 비순환 그래프
with DAG(
        "catchup",
        schedule="@hourly",
        start_date=pendulum.datetime(2025, 3, 10 , tz="Asia/Seoul"),
        catchup=False   
) as dag:
    
    Start = EmptyOperator(task_id="Start")
    End = EmptyOperator(task_id="End")
    
    Start >> End 