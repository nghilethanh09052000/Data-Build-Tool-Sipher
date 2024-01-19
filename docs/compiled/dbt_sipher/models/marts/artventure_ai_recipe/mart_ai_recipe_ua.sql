WITH cnt_new_users AS (
    SELECT
        * EXCEPT(user_id)
        ,COUNT(DISTINCT user_id) AS cnt_new_users
    FROM 
        `sipher-data-testing`.`artventure_ai_recipe`.`fct_ai_recipe_ua`
    GROUP BY 1,2,3,4,5,6,7,8,9,10
)
SELECT * 
FROM 
    cnt_new_users
ORDER BY 
    date DESC