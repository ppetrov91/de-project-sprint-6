CREATE SCHEMA IF NOT EXISTS STV202311139__STAGING;

CREATE TABLE IF NOT EXISTS STV202311139__STAGING.users (
     id int not null primary key,
     chat_name VARCHAR(200),
     registration_dt timestamp,
     country VARCHAR(200),
     age int
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES;

CREATE TABLE IF NOT EXISTS STV202311139__STAGING.groups (
     id int not null primary key,
     admin_id int,
     group_name VARCHAR(100),
     registration_dt timestamp not null,
     is_private bool
) 
ORDER BY id, admin_id
SEGMENTED BY HASH(id) ALL NODES
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__STAGING.dialogs (
     message_id int not null primary key,
     message_ts timestamp not null,
     message_from int,
     message_to int,
     message VARCHAR(1000),
     message_group int
)
SEGMENTED BY hash(message_id) all nodes
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__STAGING.group_log (
     group_id int not null,
     user_id int not null,
     user_id_from int,
     event VARCHAR(6) not null,
     datetime timestamp not null,
     CONSTRAINT group_logs_pk PRIMARY KEY(group_id, user_id, datetime)
)
ORDER BY group_id, user_id, datetime
SEGMENTED BY HASH(group_id, user_id, datetime) ALL NODES;
