DROP TABLE IF EXISTS STV202311139__STAGING.users_rej;

COPY STV202311139__STAGING.users(id, chat_name ENFORCELENGTH, registration_dt, country ENFORCELENGTH, age) 
FROM STDIN DELIMITER ',' REJECTED DATA AS TABLE STV202311139__STAGING.users_rej;
