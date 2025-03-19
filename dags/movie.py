from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        BranchPythonOperator, 
        PythonVirtualenvOperator,
)

with DAG(
    'movie',
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description='movie',
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 6),
    catchup=True,
    tags=['api', 'movie'],
) as dag:
    REQUIREMENTS = ["git+https://github.com/stundrg/movie.git@0.2.5"]
    BASE_DIR = "~/data/movies/dailyboxoffice"

    def branch_fun(ds_nodash):
        import os
        check_path = os.path.expanduser(f"{BASE_DIR}/dt={ds_nodash}")
        if os.path.exists(check_path):
            return rm_dir.task_id
        else:
            return "get.start", "echo.task"

    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun
    )
    
    def fn_merge_data(ds_nodash):
        print(ds_nodash)
        # df read => ~/temp/movie/dt=20240101
        # df1 = fill_na_with_column(df, 'multiMovieYn')
        # df2 = fill_na_with_column(df1, 'repNationCd')
        # df3 = df2.drop(columns = ['rnum','rank','rankInten','salesShare'])
        # unique_df = df3.drop_duplicates()
        # unique_df.loc[:,'rnum'] = unique_df
    
    merge_data = PythonVirtualenvOperator(
        task_id='merge.data',
        python_callable=fn_merge_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
    )
    




    def common_get_data(ds_nodash, url_param, base_path):
        from movie.api.call import call_api, list2df, save_df
        print(ds_nodash, url_param)
        
        # TO Airflow Dag
        data = call_api(dt=ds_nodash, url_param=url_param)
        df = list2df(data, ds_nodash, url_param)
        partitions = ['dt'] + list(url_param.keys())
        save_path = save_df(df, base_path, partitions)
        
        print("::group::movie df save")
        print("save_path ---->" + save_path)
        print("url_param ---->" + str(url_param))
        print("ds_nodash--->" + ds_nodash)
        print("::endgroup::")
        
        print(save_path, url_param)
        
    multi_y = PythonVirtualenvOperator(
        task_id='multi.y',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={
            "url_param": {"multiMovieYn": "Y"},
            "base_path": BASE_DIR
        }
    )

    multi_n = PythonVirtualenvOperator(
        task_id='multi.n',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={
            "url_param": {"multiMovieYn": "N"},
            "base_path": BASE_DIR
        }
    )

    nation_k = PythonVirtualenvOperator(
        task_id='nation.k',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs= {
            "url_param": {"repNationCd": "K"},
            "base_path": BASE_DIR
        }
    )

    nation_f = PythonVirtualenvOperator(
        task_id='nation.f',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs= {
            "url_param": {"repNationCd": "F"},
            "base_path": BASE_DIR
        }
    )
    
    no_param = PythonVirtualenvOperator(
        task_id='no.param',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs= {
            "url_param": {},
            "base_path": BASE_DIR
        }       
    )

    rm_dir = BashOperator(task_id='rm.dir',
                          bash_command= f'rm -rf {BASE_DIR}' + '/dt={{ ds_nodash }}')

    echo_task = BashOperator(
        task_id='echo.task',
        bash_command="echo 'task'"
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    get_start = EmptyOperator(task_id='get.start',trigger_rule="all_done")
    get_end = EmptyOperator(task_id='get.end')
    
    make_done = BashOperator(
        task_id='make.done',
        bash_command="""
        DONE_BASE=/home/wsl/data/movies/done/dailyboxoffice
        mkdir -p $DONE_BASE/{{ ds_nodash }}
        touch $DONE_BASE/{{ ds_nodash }}/_DONE
        """
    )

    start >> branch_op

    branch_op >> rm_dir >> get_start
    branch_op >> get_start
    branch_op >> echo_task
    get_start >> [multi_y, multi_n, nation_k, nation_f, no_param] >> get_end

    get_end >> merge_data >> make_done >> end