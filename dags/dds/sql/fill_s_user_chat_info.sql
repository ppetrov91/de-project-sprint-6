INSERT INTO STV202311139__DWH.s_user_chat_info(hk_user_id, chat_name, load_dt, load_src)
SELECT du.hk_user_id
     , u.chat_name
     , now() AS load_dt
     , 's3' AS load_src
  FROM STV202311139__STAGING.users u
  JOIN STV202311139__DWH.h_users du
    ON du.user_id = u.id
 WHERE NOT EXISTS (SELECT 1
                     FROM STV202311139__DWH.s_user_chat_info s
                    WHERE s.hk_user_id = du.hk_user_id
                  );
