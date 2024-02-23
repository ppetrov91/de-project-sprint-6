SELECT s.age
     , COUNT(DISTINCT s.hk_user_id) AS users_cnt
  FROM (SELECT g.hk_group_id
          FROM STV202311139__DWH.h_groups g
         ORDER BY g.registration_dt 
         LIMIT 10
       ) g
  JOIN STV202311139__DWH.l_groups_dialogs d
    ON d.hk_group_id = g.hk_group_id
  JOIN STV202311139__DWH.l_user_message m
    ON m.hk_message_id = d.hk_message_id
  JOIN STV202311139__DWH.s_user_socdem s
    ON s.hk_user_id = m.hk_user_id
 GROUP BY s.age;
