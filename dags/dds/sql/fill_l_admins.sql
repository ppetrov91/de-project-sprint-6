INSERT INTO STV202311139__DWH.l_admins(hk_l_admin_id, hk_user_id, hk_group_id, load_dt, load_src)
SELECT HASH(du.hk_user_id, dg.hk_group_id) AS hk_l_admin_id
     , du.hk_user_id
     , dg.hk_group_id
     , now() AS load_dt
     , 's3' AS load_src
  FROM STV202311139__STAGING.groups g
  JOIN STV202311139__DWH.h_users du
    ON du.user_id = g.admin_id
  JOIN STV202311139__DWH.h_groups dg
    ON dg.group_id = g.id
 WHERE NOT EXISTS (SELECT 1
		     FROM STV202311139__DWH.l_admins la
	            WHERE la.hk_l_admin_id = HASH(du.hk_user_id, dg.hk_group_id)
	          );
