import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.vertica.hooks.vertica import VerticaHook


def get_file_from_s3(conn_id, bucket_name, file_key, download_path):
    filename = S3Hook(conn_id).download_file(key=file_key, 
                                             bucket_name=bucket_name, 
                                             local_path=download_path)
    
    downloaded_filepath = os.path.join(os.path.dirname(filename), file_key)
    os.rename(src=filename, dst=downloaded_filepath)

def load_data_to_db(conn_id, sql_filepath, csv_filepath):
    with open(sql_filepath, "r") as sql_file, open(csv_filepath, "r") as csv_file:
        with VerticaHook(conn_id).get_conn() as conn, conn.cursor() as cur:
            cur.copy(sql_file.read(), csv_file, buffer_size=65536)

def execute_sql_script(conn_id, sql_filepath):
    with open(sql_filepath, "r") as sql_file:
        with VerticaHook(conn_id).get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql_file.read())
            cur.execute("COMMIT;")