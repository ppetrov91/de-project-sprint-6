INSERT INTO STV202311139__DWH.h_users(hk_user_id, user_id, registration_dt, load_dt, load_src)
SELECT HASH(u.id) AS hk_user_id
     , u.id AS user_id
     , u.registration_dt
     , now() AS load_dt
     , 's3' AS load_src
  FROM STV202311139__STAGING.users u
 WHERE NOT EXISTS (SELECT 1
		     FROM STV202311139__DWH.h_users du
	            WHERE du.hk_user_id = HASH(u.id)
	          );
