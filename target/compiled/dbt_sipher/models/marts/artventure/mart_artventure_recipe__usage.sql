WITH raw_generate AS (
    SELECT
        COALESCE(
            (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "recipe_name"),
            (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "recipe_id")
        ) as recipe,
        (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "template") AS template,
        (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "gender") AS gender,
        (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "age") AS age,
        (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "theme") AS theme,
        (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "style") AS style,
        (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "backgroundColor") AS background_color,
        (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "shapePrecisionLevel") AS shape_precision_level,
        COALESCE(
            (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "creative_intensity"),
            (SELECT ep.value.double_value FROM UNNEST(event_params) AS ep WHERE ep.key = "creative_intensity"),
            (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "creativeIntensity"),
            (SELECT ep.value.double_value FROM UNNEST(event_params) AS ep WHERE ep.key = "creativeIntensity")
        ) AS creative_intensity,
        COALESCE(
            (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "similarity"),
            (SELECT ep.value.double_value FROM UNNEST(event_params) AS ep WHERE ep.key = "similarity")
        ) AS similarity,
        (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "aspectRatio") AS aspect_ratio,
        (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "upscaleRatio") AS upscale_ratio,
        (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "denoising_strength") AS denoising_strength,
        (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "shapeControl") AS shape_control,
        (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "modelHash") AS model_hash,
        (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "pose") AS pose,
        (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "duration") AS duration,
        version,
        date,
        CASE WHEN user_id = 'anonymous' THEN user_pseudo_id ELSE user_id END AS user_id,
    FROM `sipher-data-testing`.`tmp_dbt`.`fct_artventure_user_events`
    WHERE event_name = 'click'
        AND (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "event_label") 
            IN ('recipe_alpha_generate', 'recipe-generate')
)
,reporting AS (
    SELECT
        *
    FROM raw_generate
    WHERE recipe IS NOT NULL
)

SELECT * FROM reporting