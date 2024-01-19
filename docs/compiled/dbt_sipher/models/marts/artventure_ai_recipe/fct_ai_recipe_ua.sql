WITH new_users AS (
    SELECT
        user_id
        ,recipe
        ,MIN(first_date_signin) AS date
    FROM 
        `sipher-data-testing`.`artventure_ai_recipe`.`dim_ai_recipe_users`
    GROUP BY 1,2
),
new_users_dim AS (
    SELECT
        date
        ,n.user_id
        ,n.recipe
        ,d.* EXCEPT(first_date_signin, user_id, browser, continent, traffic_medium, recipe)
    FROM new_users n
        LEFT JOIN `sipher-data-testing`.`artventure_ai_recipe`.`dim_ai_recipe_users` d
            ON n.date = d.first_date_signin AND n.user_id = d.user_id AND n.recipe = d.recipe
)
SELECT * 
FROM 
    new_users_dim