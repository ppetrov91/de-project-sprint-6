## Пояснения к проекту.

  1. Для загрузки данных из s3 в stg используется DAG save_data_to_staging.py. Наполнение таблиц схемы stg происходит путём выполнения команды COPY, скрипты можно найти в директории dags/stg/sql.
  
  2. Для наполнения данных таблиц слоя dds используется DAG load_data_to_dds.py. Сначала наполняются hubs, потом links и затем уже satelites. Скрипты наполнения можно найти в директории dags/dds/sql.
  
  3. Запрос по определению количества пользователей, который хотя бы один раз писали в самые старые группы, с группировкой по возрастам представлен в файле dags/cdm/users_by_age_in_oldest_groups.sql.
  
  4. Запрос по вычислению количества только добавленных пользователей, а также пользователей, которые хотя бы один раз писали в самые старые группы, представлен в файле dags/cdm/users_by_age_in_oldest_groups.sql.
  
  5. Скрипт определения таблиц в схеме stg представлен в файле ddl/staging_schema.sql.
  
  6. Скрипт определения таблиц в схеме dds представлен в файле ddsl/dds_schema.sql.
   
