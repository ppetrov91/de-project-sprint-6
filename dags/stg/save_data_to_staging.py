import logging
import os
from airflow.decorators import dag, task, task_group
from datetime import datetime
from lib.utils import get_file_from_s3, load_data_to_db
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable


log = logging.getLogger(__name__)
s3_conn_id = "s3_conn"
db_conn_id = "vertica_conn"

@dag(dag_id="save_data_to_staging",
     schedule_interval="0 0 * * *",
     start_date=datetime(2021, 1, 1),
     catchup=False,
     tags=["sprint6", "files_load", "project"],
     is_paused_upon_creation=True
)
def save_data_to_staging():
    bucket_name = Variable.get("BUCKET_NAME")
    download_path = Variable.get("DOWNLOAD_PATH")
    filenames = ("groups", "dialogs", "users", "group_log")
    dag_dirname = os.path.dirname(__file__)
    
    @task_group(group_id="load_data_from_s3")
    def load_data_from_s3():
        tasks = []

        for filename in filenames:
            @task(task_id=f"load_{filename}")
            def f(filename):
                csv_filename = f"{filename}.csv"
                csv_filepath = os.path.join(download_path, csv_filename)
                sql_filepath = os.path.join(dag_dirname, f"sql/download_{filename}.sql")

                if not os.path.exists(csv_filepath):
                    get_file_from_s3(conn_id=s3_conn_id, bucket_name=bucket_name, 
                                     file_key=csv_filename, download_path=download_path)
                
                load_data_to_db(conn_id=db_conn_id, sql_filepath=sql_filepath, 
                                csv_filepath=csv_filepath)

            tasks.append(f(filename=filename))

        tasks

    t_start = EmptyOperator(task_id="start")
    t_finish = EmptyOperator(task_id="finish")
    t_start >> load_data_from_s3() >> t_finish

save_data_to_staging_dag = save_data_to_staging()