INSERT INTO STV202311139__DWH.l_groups_dialogs(hk_l_group_dialogs, hk_message_id, hk_group_id, load_dt, load_src)
SELECT HASH(hd.hk_message_id, dg.hk_group_id) AS hk_l_group_dialogs
     , hd.hk_message_id
     , dg.hk_group_id
     , now() AS load_dt
     , 's3' AS load_src
  FROM STV202311139__STAGING.dialogs d
  JOIN STV202311139__DWH.h_groups dg
    ON dg.group_id = d.message_group
  JOIN STV202311139__DWH.h_dialogs hd
    ON hd.message_id = d.message_id
 WHERE NOT EXISTS (SELECT 1
		     FROM STV202311139__DWH.l_groups_dialogs lgd
	            WHERE lgd.hk_l_group_dialogs = HASH(hd.hk_message_id, dg.hk_group_id)
	          );
