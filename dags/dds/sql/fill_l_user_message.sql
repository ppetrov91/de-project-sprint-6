INSERT INTO STV202311139__DWH.l_user_message(hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
SELECT HASH(m.hk_message_id, u.hk_user_id) AS hk_l_user_message
     , u.hk_user_id
     , m.hk_message_id
     , now() AS load_dt
     , 's3' AS load_src
  FROM STV202311139__STAGING.dialogs d
  JOIN STV202311139__DWH.h_users u
    ON u.user_id = d.message_from
  JOIN STV202311139__DWH.h_dialogs m
    ON m.message_id = d.message_id
 WHERE NOT EXISTS (SELECT 1
		     FROM STV202311139__DWH.l_user_message lum
	            WHERE lum.hk_l_user_message = HASH(m.hk_message_id, u.hk_user_id)
	          );
