from pprint import pprint
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonVirtualenvOperator,
)
from airflow.sensors.filesystem import FileSensor

DAG_ID = "movie_after"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description="movie after",
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 1, 1),
    catchup=True,
    tags=["api", "movie", "sensor"],
) as dag:
    REQUIREMENTS = ["git+https://github.com/stundrg/movie.git@0.5.0"]
    BASE_DIR = f"~/data/{DAG_ID}"
    BASE_LOAD = "/home/wsl/data/movies/merge/dailyboxoffice"
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    check_done = FileSensor(
        task_id="check.done",
        filepath="/home/wsl/data/movies/done/dailyboxoffice/{{ds_nodash}}/_DONE",
        fs_conn_id="fs_after_movie",
        poke_interval=180,  # 3분마다 체크
        timeout=3600,  # 1시간 후 타임아웃
        mode="reschedule",  # 리소스를 점유하지 않고 절약하는 방식
    )

    def fn_gen_meta(base_path, base_load, ds_nodash, **kwargs):
        from movie.api.call import load_meta_data, save_meta_data, fillna_meta
        import os
        import pandas as pd
        
        meta_path = f"{base_path}/meta/meta.parquet"
        
        current_df = pd.read_parquet(f'{base_load}/dt={ds_nodash}')[
                ['movieCd', 'multiMovieYn', 'repNationCd']
            ]
        
        if os.path.exists(meta_path):
            previous_df = load_meta_data(meta_path)
            new_meta_df = fillna_meta(previous_df, current_df)
        else:
            new_meta_df = current_df
        
        save_path = save_meta_data(base_path, new_meta_df)
        print(f"✅ 메타 데이터 저장 완료 : {save_path}")     

    gen_meta = PythonVirtualenvOperator(
        task_id="gen.meta",
        python_callable=fn_gen_meta,
        requirements=REQUIREMENTS,
        
        system_site_packages=False,
        op_kwargs={
            "base_path": BASE_DIR,
            "base_load": BASE_LOAD
            },
        
    )

    def fn_gen_movie(base_path, base_load, ds_nodash, **kwargs):
        """ 
        하루 단위 데이터를 메타데이터와 병합하여 저장 
        """
        print(base_load, ds_nodash, base_path)
        from movie.api.call import load_meta_data, save_df
        import pandas as pd
        print("✅ import 완료")
    
        save_path = f"{base_path}/dailyboxoffice"
        
        meta_df = load_meta_data(base_path) # 50 rows
        current_df = pd.read_parquet(f'{base_load}/dt={ds_nodash}') # 30 rows
        
        merged_df = current_df.merge(meta_df, on="movieCd", how="left", suffixes=("_current", "_meta"))
        merged_df["multiMovieYn"] = merged_df["multiMovieYn_meta"].combine_first(merged_df["multiMovieYn_current"])
        merged_df["repNationCd"] = merged_df["repNationCd_meta"].combine_first(merged_df["repNationCd_current"])
        final_df = merged_df[current_df.columns]
        final_df['dt'] = ds_nodash
        print(final_df)
        print(len(final_df), len(current_df))
        save_df(final_df, save_path, partitions=["dt", "multiMovieYn", "repNationCd"])
        print(f"✅ 데이터 저장 완료")
        
            
        # # ✅ 저장 전, 컬럼 확인
        # print("✅ 저장 전 컬럼 확인:", meta_df.columns.tolist())
        
        # if meta_df is not None:
        #     save_df(meta_df, save_path, partitions=["dt", "multiMovieYn", "repNationCd"])
        #     print(f"✅ 데이터 저장 완료: {save_path}/dt={ds_nodash}")
        # else:
        #     print(f"❌ 병합된 데이터 없음: {save_path}/dt={ds_nodash}")

    gen_movie = PythonVirtualenvOperator(
        task_id="gen.movie",
        python_callable=fn_gen_movie,
        requirements=REQUIREMENTS,
        system_site_packages=False,
        op_kwargs={
            "base_path": BASE_DIR,
            "base_load": BASE_LOAD
        }
    )

    make_done = BashOperator(
        task_id="make.done",
        bash_command="""
        DONE_BASE=$BASE_DIR/done
        echo $DONE_BASE
        mkdir -p $DONE_BASE/{{ ds_nodash }}
        touch $DONE_BASE/{{ ds_nodash }}/_DONE
        """,
        env={'BASE_DIR':BASE_DIR},
        append_env = True
    )

    start >> check_done >> gen_meta >> gen_movie >> make_done >> end
