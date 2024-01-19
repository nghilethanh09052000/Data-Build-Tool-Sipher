WITH user_generate AS (
    SELECT
        date
        ,user_id
        ,recipe
        ,ROW_NUMBER() OVER(PARTITION BY user_id, recipe ORDER BY date) AS generate_times
    FROM 
        `sipher-data-testing`.`artventure_ai_recipe`.`fct_ai_recipe_ue`
    WHERE 
        (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "event_label") = 'recipe_alpha_generate' 
), final AS (
    SELECT
        date
        ,recipe
        ,generate_times          AS freq
        ,COUNT(DISTINCT user_id) AS cnt
    FROM 
        user_generate
    GROUP BY 1,2,3
)

SELECT 
    * 
FROM 
    final