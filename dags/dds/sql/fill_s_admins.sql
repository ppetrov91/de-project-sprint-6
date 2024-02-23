INSERT INTO STV202311139__DWH.s_admins(hk_l_admin_id, is_admin, admin_from, load_dt, load_src)
SELECT l.hk_l_admin_id
     , true AS is_admin
     , g.registration_dt AS admin_from
     , now() AS load_dt
     , 's3' AS load_src
  FROM STV202311139__DWH.h_groups g
  JOIN STV202311139__DWH.l_admins l
    ON l.hk_group_id = g.hk_group_id
 WHERE NOT EXISTS (SELECT 1
		     FROM STV202311139__DWH.s_admins a
	            WHERE a.hk_l_admin_id = l.hk_l_admin_id
	              AND a.admin_from = g.registration_dt
	          );
