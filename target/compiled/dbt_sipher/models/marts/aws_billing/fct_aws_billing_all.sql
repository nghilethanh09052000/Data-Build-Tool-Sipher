

WITH

  extracted_date_month_blockchain AS(
    SELECT
      DISTINCT MAX(snapshot_date_tzutc) AS max_dt,
      EXTRACT(MONTH FROM snapshot_date_tzutc) AS act_month
    FROM `sipher-data-testing`.`raw_aws_billing_gcs`.`stg_aws__billing__raw_blockchain`
    GROUP BY 2
  )

  ,blockchain AS
  (SELECT
    bill_billing_period_start_date,
    line_item_usage_start_date,
    line_item_product_code,
    SUM(CAST(line_item_unblended_cost AS NUMERIC)) AS line_item_unblended_cost,
    SUM(CAST(line_item_blended_cost AS NUMERIC)) AS line_item_blended_cost,
    line_item_line_item_description,
    'blockchain' AS aws_account,
    MAX(snapshot_date_tzutc) AS partition_date
  FROM `sipher-data-testing`.`raw_aws_billing_gcs`.`stg_aws__billing__raw_blockchain`
  WHERE line_item_line_item_type = 'Usage'
    AND snapshot_date_tzutc IN (SELECT max_dt FROM extracted_date_month_blockchain)
  GROUP BY 1,2,3,6
  ORDER BY 2)


  ,extracted_date_month_g1 AS(
    SELECT
      DISTINCT MAX(snapshot_date_tzutc) AS max_dt,
      EXTRACT(MONTH FROM snapshot_date_tzutc) AS act_month
    FROM `sipher-data-testing`.`raw_aws_billing_gcs`.`stg_aws__billing__raw_g1`
    GROUP BY 2
  )

  ,g1 AS
  (SELECT
    bill_billing_period_start_date,
    line_item_usage_start_date,
    line_item_product_code,
    SUM(CAST(line_item_unblended_cost AS NUMERIC)) AS line_item_unblended_cost,
    SUM(CAST(line_item_blended_cost AS NUMERIC)) AS line_item_blended_cost,
    line_item_line_item_description,
    'g1' AS aws_account,
    MAX(snapshot_date_tzutc) AS partition_date
  FROM `sipher-data-testing`.`raw_aws_billing_gcs`.`stg_aws__billing__raw_g1`
  WHERE line_item_line_item_type = 'Usage'
    AND snapshot_date_tzutc IN (SELECT max_dt FROM extracted_date_month_g1)
  GROUP BY 1,2,3,6
  ORDER BY 2)


  ,extracted_date_month_marketplace AS(
    SELECT
      DISTINCT MAX(snapshot_date_tzutc) AS max_dt,
      EXTRACT(MONTH FROM snapshot_date_tzutc) AS act_month
    FROM `sipher-data-testing`.`raw_aws_billing_gcs`.`stg_aws__billing__raw_marketplace`
    GROUP BY 2
  )

  ,marketplace AS
  (SELECT
    bill_billing_period_start_date,
    line_item_usage_start_date,
    line_item_product_code,
    SUM(CAST(line_item_unblended_cost AS NUMERIC)) AS line_item_unblended_cost,
    SUM(CAST(line_item_blended_cost AS NUMERIC)) AS line_item_blended_cost,
    line_item_line_item_description,
    'marketplace' AS aws_account,
    MAX(snapshot_date_tzutc) AS partition_date
  FROM `sipher-data-testing`.`raw_aws_billing_gcs`.`stg_aws__billing__raw_marketplace`
  WHERE line_item_line_item_type = 'Usage'
    AND snapshot_date_tzutc IN (SELECT max_dt FROM extracted_date_month_marketplace)
  GROUP BY 1,2,3,6
  ORDER BY 2)


  ,extracted_date_month_game_production AS(
    SELECT
      DISTINCT MAX(snapshot_date_tzutc) AS max_dt,
      EXTRACT(MONTH FROM snapshot_date_tzutc) AS act_month
    FROM `sipher-data-testing`.`raw_aws_billing_gcs`.`stg_aws__billing__raw_game_production`
    GROUP BY 2
  )

  ,game_production AS
  (SELECT
    bill_billing_period_start_date,
    line_item_usage_start_date,
    line_item_product_code,
    SUM(CAST(line_item_unblended_cost AS NUMERIC)) AS line_item_unblended_cost,
    SUM(CAST(line_item_blended_cost AS NUMERIC)) AS line_item_blended_cost,
    line_item_line_item_description,
    'game_production' AS aws_account,
    MAX(snapshot_date_tzutc) AS partition_date
  FROM `sipher-data-testing`.`raw_aws_billing_gcs`.`stg_aws__billing__raw_game_production`
  WHERE line_item_line_item_type = 'Usage'
    AND snapshot_date_tzutc IN (SELECT max_dt FROM extracted_date_month_game_production)
  GROUP BY 1,2,3,6
  ORDER BY 2)



  SELECT * FROM marketplace
  UNION ALL
  SELECT * FROM g1
  UNION ALL
  SELECT * FROM blockchain