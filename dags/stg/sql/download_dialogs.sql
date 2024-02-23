DROP TABLE IF EXISTS STV202311139__STAGING.dialogs_rej;

COPY STV202311139__STAGING.dialogs(message_id, message_ts, message_from, message_to, 
	                           message ENFORCELENGTH ENCLOSED '"', message_group) 
FROM STDIN DELIMITER ',' REJECTED DATA AS TABLE STV202311139__STAGING.dialogs_rej;
