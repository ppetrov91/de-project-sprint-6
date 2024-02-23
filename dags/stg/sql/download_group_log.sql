DROP TABLE IF EXISTS STV202311139__STAGING.group_log_rej;

COPY STV202311139__STAGING.group_log(group_id, user_id, user_id_from, event, datetime) 
FROM STDIN DELIMITER ',' REJECTED DATA AS TABLE STV202311139__STAGING.group_log_rej;
