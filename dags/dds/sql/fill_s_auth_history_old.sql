INSERT INTO STV202311139__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event_name, event_dt, load_dt, load_src)
SELECT luga.hk_l_user_group_activity
     , gl.user_id_from
     , gl.event_name
     , gl.event_ts AS event_dt
     , now() AS load_dt
     , 's3' AS load_src
  FROM STV202311139__STAGING.group_log gl
  JOIN STV202311139__DWH.h_users hu
    ON hu.user_id = gl.user_id
  JOIN STV202311139__DWH.h_groups hg
    ON hg.group_id = gl.group_id
  JOIN STV202311139__DWH.l_user_group_activity luga
    ON hg.hk_group_id = luga.hk_group_id
   AND hu.hk_user_id = luga.hk_user_id
 WHERE NOT EXISTS (SELECT 1
		     FROM STV202311139__DWH.s_auth_history h
	            WHERE h.hk_l_user_group_activity = luga.hk_l_user_group_activity
	              AND h.event_dt = gl.event_ts
	          );
