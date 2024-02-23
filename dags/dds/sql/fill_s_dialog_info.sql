INSERT INTO STV202311139__DWH.s_dialog_info(hk_message_id, message, message_from, message_to, load_dt, load_src)
SELECT h.hk_message_id
     , d.message
     , d.message_from
     , d.message_to
     , now() AS load_dt
     , 's3' AS load_src
  FROM STV202311139__STAGING.dialogs d
  JOIN STV202311139__DWH.h_dialogs h
    ON h.message_id = d.message_id
 WHERE NOT EXISTS (SELECT 1
		     FROM STV202311139__DWH.s_dialog_info sdi
	            WHERE sdi.hk_message_id = h.hk_message_id
	          );
