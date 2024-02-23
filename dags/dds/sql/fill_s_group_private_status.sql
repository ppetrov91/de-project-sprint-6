INSERT INTO STV202311139__DWH.s_group_private_status(hk_group_id, is_private, load_dt, load_src)
SELECT dg.hk_group_id
     , sg.is_private
     , now() AS load_dt
     , 's3' AS load_src
  FROM STV202311139__STAGING.groups sg
  JOIN STV202311139__DWH.h_groups dg
    ON dg.group_id = sg.id
 WHERE NOT EXISTS (SELECT 1
		     FROM STV202311139__DWH.s_group_private_status sgn
	            WHERE sgn.hk_group_id = dg.hk_group_id
	          );
