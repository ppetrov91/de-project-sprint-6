INSERT INTO STV202311139__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event_name, event_dt, load_dt, load_src)
SELECT HASH(u.hk_user_id, g.hk_group_id) AS hk_l_user_group_activity
     , gl.user_id_from
     , gl.event_name
     , gl.event_ts AS event_dt
     , now() AS load_dt
     , 's3' AS load_src
  FROM STV202311139__STAGING.group_log gl
  JOIN STV202311139__DWH.h_users u
    ON u.user_id = gl.user_id
  JOIN STV202311139__DWH.h_groups g
    ON g.group_id = gl.group_id
 WHERE NOT EXISTS (SELECT 1
		     FROM STV202311139__DWH.s_auth_history h
	            WHERE h.hk_l_user_group_activity = HASH(u.hk_user_id, g.hk_group_id)
	              AND h.event_dt = gl.event_ts
	          );
