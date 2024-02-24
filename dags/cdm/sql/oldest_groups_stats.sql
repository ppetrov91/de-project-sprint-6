WITH groups AS (
SELECT g.hk_group_id
     , g.registration_dt
  FROM STV202311139__DWH.h_groups g
 ORDER BY g.registration_dt
 LIMIT 10
), 
user_group_messages AS (
SELECT g.hk_group_id
     , COUNT(DISTINCT l.hk_user_id) AS cnt_users_in_group_with_messages
  FROM groups g
  LEFT JOIN STV202311139__DWH.l_groups_dialogs gd
    ON gd.hk_group_id = g.hk_group_id
  LEFT JOIN STV202311139__DWH.l_user_message l
    ON l.hk_message_id = gd.hk_message_id
 GROUP BY g.hk_group_id
),
user_group_log AS (
SELECT v.hk_group_id
     , COUNT(DISTINCT v.hk_user_id) AS cnt_added_users
  FROM (SELECT MAX(g.hk_group_id) AS hk_group_id
             , MAX(ga.hk_user_id) AS hk_user_id
          FROM groups g
          JOIN STV202311139__DWH.l_user_group_activity ga
            ON ga.hk_group_id = g.hk_group_id
          JOIN STV202311139__DWH.s_auth_history s
            ON s.hk_l_user_group_activity = ga.hk_l_user_group_activity
         GROUP BY s.hk_l_user_group_activity
        HAVING COUNT(CASE WHEN s.event_name = 'add' THEN 1 END) > 0
           AND COUNT(CASE WHEN s.event_name != 'add' THEN 1 END) = 0
       ) v
 GROUP BY v.hk_group_id
)
SELECT ugl.hk_group_id
     , ugl.cnt_added_users
     , ugm.cnt_users_in_group_with_messages
     , ugm.cnt_users_in_group_with_messages / NULLIF(ugl.cnt_added_users, 0) AS group_conversion 
  FROM user_group_messages ugm
  LEFT JOIN user_group_log ugl
    ON ugm.hk_group_id = ugl.hk_group_id
 ORDER BY group_conversion DESC;
