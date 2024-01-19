WITH referral AS (
    SELECT
        date
        ,REGEXP_EXTRACT(
            (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "event_label")     
            , r'_(.*?)_'
        ) AS platform
        ,recipe
        ,COUNT(*) AS cnt_referral
    FROM 
        `sipher-data-testing`.`artventure_ai_recipe`.`fct_ai_recipe_ue`
    WHERE 
        event_name = 'click'
        AND (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "event_label") LIKE '%referralCode%'
    GROUP BY 1,2,3
),
share AS (
    SELECT
        date
        ,(SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "platform") AS platform  
        ,recipe
        ,COUNT(*) AS cnt_share
    FROM 
        `sipher-data-testing`.`artventure_ai_recipe`.`fct_ai_recipe_ue`
    WHERE 
        event_name = 'click'
        AND (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "event_label") = 'share_social'
    GROUP BY 1,2,3
),
final AS (
    SELECT
        s.date
        ,s.platform
        ,s.recipe
        ,COALESCE(r.cnt_referral, 0) AS cnt_refer
        ,COALESCE(s.cnt_share, 0)    AS cnt_share
    FROM referral r
        FULL OUTER JOIN share s ON r.date = s.date
)

SELECT 
    * 
FROM 
    final
ORDER BY 
    date DESC