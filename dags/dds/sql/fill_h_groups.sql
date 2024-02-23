INSERT INTO STV202311139__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
SELECT HASH(sg.id) AS hk_group_id
     , sg.id AS group_id
     , sg.registration_dt
     , now() AS load_dt
     , 's3' AS load_src
  FROM STV202311139__STAGING.groups sg
 WHERE NOT EXISTS (SELECT 1
		     FROM STV202311139__DWH.h_groups hg
	            WHERE hg.hk_group_id = HASH(sg.id)
	          );
