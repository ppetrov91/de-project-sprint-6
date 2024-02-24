INSERT INTO STV202311139__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
SELECT DISTINCT HASH(u.hk_user_id, g.hk_group_id) AS hk_l_user_group_activity
     , u.hk_user_id
     , g.hk_group_id
     , now() AS load_dt
     , 's3' AS load_src
  FROM STV202311139__STAGING.group_log gl
  JOIN STV202311139__DWH.h_users u
    ON u.user_id = gl.user_id
  JOIN STV202311139__DWH.h_groups g
    ON g.group_id = gl.group_id
 WHERE NOT EXISTS (SELECT 1
		     FROM STV202311139__DWH.l_user_group_activity uga
	            WHERE uga.hk_l_user_group_activity = HASH(u.hk_user_id, g.hk_group_id)
	          );
