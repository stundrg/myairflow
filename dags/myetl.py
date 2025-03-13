from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator
import pendulum

# ✅ DAG 설정
with DAG(
    "myetl",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 13, tz="Asia/Seoul"),
    default_args={"depends_on_past": False},  # 🔥 실행 안정성 추가
    max_active_runs=1,  # 🔥 동시에 여러 개 실행 방지
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    make_data = BashOperator(
        task_id="make_data",
        bash_command="""
            echo "make data"
            bash /home/wsl/airflow/make_data.sh /home/wsl/data/{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}
        """,
    )

    # ✅ CSV → Parquet 변환
    def run_load_data(input_path):
        """CSV 데이터를 Parquet으로 변환하는 함수"""
        from myetl.load_data import load_data  
        output_path = input_path.replace("data.csv", "data.parquet")  
        print(f"✅ Load Data 실행: {input_path} → {output_path}")
        load_data(input_path, output_path)

    load_data = PythonVirtualenvOperator(
        task_id="load_data",
        python_callable=run_load_data,
        requirements=[
            "git+https://github.com/stundrg/myetl.git@0.1.0",  
        ],
        system_site_packages=True,  
        op_args=["/home/wsl/data/{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}/data.csv"],  
    )

    # ✅ Parquet → Aggregation 후 CSV 저장
    def run_agg_data(input_path):
        """Parquet 데이터를 Aggregation 후 CSV로 변환하는 함수"""
        from myetl.agg_data import agg_data  # 
        output_path = input_path.replace("data.parquet", "agg.csv") 
        print(f"✅ Agg Data 실행: {input_path} → {output_path}")
        agg_data(input_path, output_path)

    agg_data = PythonVirtualenvOperator(
        task_id="agg_data",
        python_callable=run_agg_data,
        requirements=[
            "git+https://github.com/stundrg/myetl.git@0.1.0",  
        ],
        system_site_packages=True, 
        op_args=["/home/wsl/data/{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}/data.parquet"],  
    )

    start >> make_data >> load_data >> agg_data >> end

if __name__ == "__main__":
    dag.test()
