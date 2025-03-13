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
            echo "make data"
            bash /home/wsl/airflow/make_data.sh /home/wsl/data/{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}
        """
    )
    
    def f_load_data(path):
        print(path, "=====================>")
        from myairflow.imsi import my_plus
        print(my_plus(1, 2), "+++++++++++++++++++++++")

    load_data = PythonVirtualenvOperator(
        task_id="load_data",
        python_callable=f_load_data,
        requirements=["git+https://github.com/stundrg/myairflow.git@0.1.0"],
        op_args=["{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}"],
        )
    
    def f_agg_data(path):
        print(path, "=====================>")
        from myairflow.imsi import my_plus
        print(my_plus(1, 2), "+++++++++++++++++++++++")
        
    agg_data = PythonVirtualenvOperator(
        task_id="agg_data",
        python_callable=f_agg_data,
        requirements=["git+https://github.com/stundrg/myairflow.git@0.1.0"],
        op_args=["{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}"],
        )
    
    start >> make_data >> load_data >> end
    
if __name__ == "__main__":
    dag.test()