from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

# Directed Acyclic Graph 한글로 번역하면 방향성이 있는 비순환 그래프
with DAG("hello",
        # schedule=timedelta(days=1),
        schedule="* * * * *",
        # schedule="@hourly",
        # schedule="@hourly",
        start_date=datetime(2025, 3, 10)
        
) as dag:
    
    Start = EmptyOperator(task_id="Start")
    End = EmptyOperator(task_id="End")
    
    Start >> End 
