from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
import pendulum


# Directed Acyclic Graph
with DAG(
    "virtual",
    schedule="@hourly",
    # start_date=datetime(2025, 3, 10)
    start_date=pendulum.datetime(2025, 3, 13, tz="Asia/Seoul")
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
   
    make_data = BashOperator(
        task_id="b_1", 
        bash_command=f"""
            echo "TODO"
        """
    )
    
    def f_load_data(path):
        print(path, "=====================>")
        import time
        time.sleep(60)

    load_data = PythonVirtualenvOperator(
        task_id="load_data",
        python_callable=f_load_data,
        requirements=[],
        op_args=["{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}"],
        )
    
    start >> make_data >> load_data >> end
    
if __name__ == "__main__":
    dag.test()