CREATE SCHEMA IF NOT EXISTS STV202311139__DWH;

CREATE TABLE IF NOT EXISTS STV202311139__DWH.h_users (
    hk_user_id int not null primary key,
    user_id int not null UNIQUE,
    registration_dt timestamp,
    load_dt timestamp not null,
    load_src varchar(20) not null
)
ORDER BY load_dt
SEGMENTED BY HASH(user_id) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__DWH.h_groups (
     hk_group_id int not null primary key,
     group_id int not null UNIQUE,
     registration_dt timestamp not null,
     load_dt timestamp not null,
     load_src varchar(20) not null
)
ORDER BY load_dt
SEGMENTED BY HASH(group_id) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__DWH.h_dialogs (
     hk_message_id int not null primary key,
     message_id int not null UNIQUE,
     message_ts timestamp not null,
     load_dt timestamp not null,
     load_src varchar(20) not null
)
order by load_dt
SEGMENTED BY HASH(message_id) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__DWH.l_user_group_activity (
     hk_l_user_group_activity int not null primary key,
     hk_user_id int not null,
     hk_group_id int not null,
     load_dt timestamp not null,
     load_src varchar(20) not null,
     CONSTRAINT l_user_group_activity_hk_user_id_fk FOREIGN KEY(hk_user_id) REFERENCES STV202311139__DWH.h_users(hk_user_id),
     CONSTRAINT l_user_group_activity_hk_group_id_fk FOREIGN KEY(hk_group_id) REFERENCES STV202311139__DWH.h_groups(hk_group_id)
)
order by load_dt
SEGMENTED BY HASH(hk_user_id, hk_group_id) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__DWH.l_user_message (
     hk_l_user_message int not null primary key,
     hk_user_id int not null,
     hk_message_id int not null,
     load_dt timestamp not null,
     load_src varchar(20) not null,
     CONSTRAINT l_user_message_hk_user_id_fk FOREIGN KEY(hk_user_id) REFERENCES STV202311139__DWH.h_users(hk_user_id),
     CONSTRAINT l_user_message_hk_message_id_fk FOREIGN KEY(hk_message_id) REFERENCES STV202311139__DWH.h_dialogs(hk_message_id)
)
order by load_dt
SEGMENTED BY HASH(hk_message_id, hk_user_id) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__DWH.l_admins (
     hk_l_admin_id int not null primary key,
     hk_user_id int not null,
     hk_group_id int not null,
     load_dt timestamp not null,
     load_src varchar(20) not null,
     CONSTRAINT l_admins_hk_user_id_fk FOREIGN KEY(hk_user_id) REFERENCES STV202311139__DWH.h_users(hk_user_id),
     CONSTRAINT l_admins_hk_group_id_fk FOREIGN KEY(hk_group_id) REFERENCES STV202311139__DWH.h_groups(hk_group_id)
)
order by load_dt
SEGMENTED BY HASH(hk_user_id, hk_group_id) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__DWH.l_groups_dialogs (
     hk_l_group_dialogs int not null primary key,
     hk_message_id int not null,
     hk_group_id int not null,
     load_dt timestamp not null,
     load_src varchar(20) not null,
     CONSTRAINT l_gd_hk_message_id_fk FOREIGN KEY(hk_message_id) REFERENCES STV202311139__DWH.h_dialogs(hk_message_id),
     CONSTRAINT l_gd_hk_group_id_fk FOREIGN KEY(hk_group_id) REFERENCES STV202311139__DWH.h_groups(hk_group_id)
)
order by load_dt
SEGMENTED BY HASH(hk_message_id, hk_group_id) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__DWH.s_auth_history (
     hk_l_user_group_activity int not null,
     user_id_from int,
     event_name varchar(6) not null,
     event_dt timestamp not null,
     load_dt timestamp not null,
     load_src varchar(20) not null,
     CONSTRAINT sah_hk_l_uga_fk FOREIGN KEY (hk_l_user_group_activity) 
     REFERENCES STV202311139__DWH.l_user_group_activity(hk_l_user_group_activity)
)
order by event_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY event_dt::date
GROUP BY calendar_hierarchy_day(event_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__DWH.s_user_socdem (
     hk_user_id int not null,
     chat_name varchar(200) not null,
     country varchar(200) not null,
     age int not null,
     load_dt timestamp not null,
     load_src varchar(20) not null,
     CONSTRAINT sus_hk_user_id_fk FOREIGN KEY(hk_user_id) REFERENCES STV202311139__DWH.h_users(hk_user_id)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__DWH.s_user_chat_info (
     hk_user_id int not null,
     chat_name varchar(200) not null,
     load_dt timestamp not null,
     load_src varchar(20) not null,
     CONSTRAINT suc_hk_user_id_fk FOREIGN KEY(hk_user_id) REFERENCES STV202311139__DWH.h_users(hk_user_id)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__DWH.s_admins (
     hk_l_admin_id int not null,
     is_admin bool not null,
     admin_from timestamp not null,
     load_dt timestamp not null,
     load_src varchar(20) not null,
     CONSTRAINT sa_hk_l_admin_id_fk FOREIGN KEY(hk_l_admin_id) REFERENCES STV202311139__DWH.l_admins(hk_l_admin_id)
)
order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__DWH.s_group_name (
    hk_group_id int not null,
    group_name varchar(100) not null,
    load_dt timestamp not null,
    load_src varchar(20) not null,
    CONSTRAINT sgn_hk_group_id_fk FOREIGN KEY(hk_group_id) REFERENCES STV202311139__DWH.h_groups (hk_group_id)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__DWH.s_group_private_status (
    hk_group_id int not null,
    is_private bool not null,
    load_dt timestamp not null,
    load_src varchar(20) not null,
    CONSTRAINT sgps_hk_group_id_fk FOREIGN KEY(hk_group_id) REFERENCES STV202311139__DWH.h_groups (hk_group_id)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__DWH.s_dialog_info (
    hk_message_id int not null,
    message varchar(1000),
    message_from int not null,
    message_to int not null,
    load_dt timestamp not null,
    load_src varchar(20) not null,
    CONSTRAINT sdi_hk_message_id_fk FOREIGN KEY(hk_message_id) REFERENCES STV202311139__DWH.h_dialogs(hk_message_id)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
