{{- config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "created_at_date",
        "data_type": "date",
        "granularity": "day"
    },
) -}}

WITH adj AS (
    SELECT  
        EXTRACT(DATE FROM created_at) AS created_at_date
        , app_name_dashboard
        , app_version
        , environment
        , os_name
        , device_type
        , country
        , network_name
        , campaign_name
        , activity_kind
        , event_name
        , reporting_currency
        , SUM(reporting_revenue) AS revenue
        , SUM(reporting_cost) AS cost
        , COUNT(*) AS count
        , JSON_EXTRACT_SCALAR(publisher_parameters, '$.user_id') AS user_id
    FROM 
        {{ ref('stg_adjust')}}
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,16
    ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12,16
)

SELECT * FROM adj
{% if is_incremental() %}
WHERE created_at_date >= (SELECT MAX(created_at_date) FROM {{ this }})
{% endif %}
