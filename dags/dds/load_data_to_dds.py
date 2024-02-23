import logging
import os
from airflow.decorators import dag, task, task_group
from datetime import datetime
from lib.utils import execute_sql_script
from airflow.operators.empty import EmptyOperator


log = logging.getLogger(__name__)
db_conn_id = "vertica_conn"

@dag(dag_id="load_data_to_dds",
     schedule_interval="0 0 * * *",
     start_date=datetime(2021, 1, 1),
     catchup=False,
     tags=["sprint6", "files_load", "project"],
     is_paused_upon_creation=True
)
def save_data_to_staging():
    lst = []
    hubs = ("h_dialogs", "h_groups", "h_users")
    links = ("l_admins", "l_groups_dialogs", "l_user_group_activity", "l_user_message")
    satelites = ("s_admins", "s_auth_history", "s_dialog_info", "s_group_name",
                 "s_group_private_status", "s_user_chat_info", "s_user_socdem")
    
    d = {"hubs": hubs, "links": links, "satelites": satelites}

    dag_dirname = os.path.dirname(__file__)
    
    for k, v in d.items():
        @task_group(group_id=k)
        def load_data(objects):
            tasks = []

            for obj in objects:
                @task(task_id=f"load_{obj}")
                def f(obj):
                    sql_filepath = os.path.join(dag_dirname, f"sql/fill_{obj}.sql")
                    execute_sql_script(conn_id=db_conn_id, sql_filepath=sql_filepath)

                tasks.append(f(obj=obj))

            tasks

        lst.append(load_data(v))

    t_start = EmptyOperator(task_id="start")
    t_finish = EmptyOperator(task_id="finish")
    t_start >> lst[0] >> lst[1] >> lst[2] >> t_finish

save_data_to_staging_dag = save_data_to_staging()