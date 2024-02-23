INSERT INTO STV202311139__DWH.h_dialogs(hk_message_id, message_id, message_ts, load_dt, load_src)
SELECT HASH(d.message_id) AS hk_message_id
     , d.message_id
     , d.message_ts
     , now() AS load_dt
     , 's3' AS load_src
  FROM STV202311139__STAGING.dialogs d
 WHERE NOT EXISTS (SELECT 1
		     FROM STV202311139__DWH.h_dialogs hd
	            WHERE hd.hk_message_id = HASH(d.message_id)
	          );
