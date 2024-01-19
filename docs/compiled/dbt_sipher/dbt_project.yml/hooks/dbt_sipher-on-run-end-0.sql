
  
  create or replace table sipher-data-testing.tmp_dbt.test_results_central as (
    
    select
      cast('expect_column_values_to_be_of_type' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.blueprint' as string) as source_refs,
      cast('tier' as string) as column_names,
      cast('dbt_expectations_source_expect_column_values_to_be_of_type_raw_meta_equipment_blueprint_tier__int64' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('6.028561592102051' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    union all
    
    select
      cast('expect_column_values_to_be_of_type' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.capsule' as string) as source_refs,
      cast('tier' as string) as column_names,
      cast('dbt_expectations_source_expect_column_values_to_be_of_type_raw_meta_equipment_capsule_tier__int64' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('6.164678335189819' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    union all
    
    select
      cast('accepted_values' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.blueprint' as string) as source_refs,
      cast('weaponType' as string) as column_names,
      cast('source_accepted_values_raw_meta_equipment_blueprint_weaponType__None' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('6.399113655090332' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    union all
    
    select
      cast('expect_column_values_to_be_of_type' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.equipment_part' as string) as source_refs,
      cast('tier' as string) as column_names,
      cast('dbt_expectations_source_expect_column_values_to_be_of_type_raw_meta_equipment_equipment_part_tier__int64' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('6.537100315093994' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    union all
    
    select
      cast('expect_column_values_to_be_of_type' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.equipment' as string) as source_refs,
      cast('tier' as string) as column_names,
      cast('dbt_expectations_source_expect_column_values_to_be_of_type_raw_meta_equipment_equipment_tier__int64' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('6.735363006591797' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    union all
    
    select
      cast('accepted_values' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.blueprint' as string) as source_refs,
      cast('type' as string) as column_names,
      cast('source_accepted_values_raw_meta_equipment_blueprint_type__Gear' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('6.759697198867798' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    union all
    
    select
      cast('expect_column_values_to_be_of_type' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.currency' as string) as source_refs,
      cast('tier' as string) as column_names,
      cast('dbt_expectations_source_expect_column_values_to_be_of_type_raw_meta_equipment_currency_tier__int64' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('6.961452484130859' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    union all
    
    select
      cast('expect_column_values_to_be_of_type' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.weapon' as string) as source_refs,
      cast('tier' as string) as column_names,
      cast('dbt_expectations_source_expect_column_values_to_be_of_type_raw_meta_equipment_weapon_tier__int64' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('7.026742696762085' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    union all
    
    select
      cast('accepted_values' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.blueprint' as string) as source_refs,
      cast('race' as string) as column_names,
      cast('source_accepted_values_raw_meta_equipment_blueprint_race__None' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('7.071630001068115' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    union all
    
    select
      cast('expect_column_values_to_be_of_type' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.character' as string) as source_refs,
      cast('tier' as string) as column_names,
      cast('dbt_expectations_source_expect_column_values_to_be_of_type_raw_meta_equipment_character_tier__int64' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('7.268376588821411' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    union all
    
    select
      cast('not_null' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.sipher_game_reward' as string) as source_refs,
      cast('id' as string) as column_names,
      cast('source_not_null_raw_meta_equipment_sipher_game_reward_id' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('5.156773090362549' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    union all
    
    select
      cast('not_null' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.capsule' as string) as source_refs,
      cast('id' as string) as column_names,
      cast('source_not_null_raw_meta_equipment_capsule_id' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('6.5345985889434814' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    union all
    
    select
      cast('not_null' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.equipment' as string) as source_refs,
      cast('id' as string) as column_names,
      cast('source_not_null_raw_meta_equipment_equipment_id' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('6.263275146484375' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    union all
    
    select
      cast('not_null' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.weapon' as string) as source_refs,
      cast('id' as string) as column_names,
      cast('source_not_null_raw_meta_equipment_weapon_id' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('6.2189977169036865' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    union all
    
    select
      cast('not_null' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.currency' as string) as source_refs,
      cast('id' as string) as column_names,
      cast('source_not_null_raw_meta_equipment_currency_id' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('6.613799571990967' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    union all
    
    select
      cast('not_null' as string) as test_name,
      cast('ERROR' as string) as test_severity_config,
      cast('pass' as string) as test_result,
      cast('' as string) as model_refs,
      cast('raw_meta_equipment.character' as string) as source_refs,
      cast('id' as string) as column_names,
      cast('source_not_null_raw_meta_equipment_character_id' as string) as test_name_long,
      cast('generic' as string) as test_type,
      cast('7.279100656509399' as string) as execution_time_seconds,
      cast('models/staging/sipher_meta_equipment/src_sipher_meta_equipment.yml' as string) as file_test_defined,
      cast('variable_not_set' as string) as pipeline_name,
      cast('variable_not_set' as string) as pipeline_type,
      cast('dev' as string) as dbt_cloud_target_name,
      cast('manual' as string) as _audit_project_id,
      cast('manual' as string) as _audit_job_id,
      cast('manual' as string) as _audit_run_id,
      'https://cloud.getdbt.com/#/accounts/account_id/projects/'||cast('manual' as string) ||'/runs/'|| cast('manual' as string) as _audit_run_url,
      current_timestamp as _timestamp
    
  
  );

  
      create table if not exists sipher-data-testing.tmp_dbt.test_results_history as (
        select 
       -- must change into generate_surrogate_key, otherwise it will be ERROR
          to_hex(md5(cast(coalesce(cast(test_name as STRING), '') || '-' || coalesce(cast(test_result as STRING), '') || '-' || coalesce(cast(_timestamp as STRING), '') as STRING))) as sk_id, 
          * 
        from sipher-data-testing.tmp_dbt.test_results_central
        where false
      );

    insert into sipher-data-testing.tmp_dbt.test_results_history 
      select 
       to_hex(md5(cast(coalesce(cast(test_name as STRING), '') || '-' || coalesce(cast(test_result as STRING), '') || '-' || coalesce(cast(_timestamp as STRING), '') as STRING))) as sk_id, 
       * 
      from sipher-data-testing.tmp_dbt.test_results_central
    ;
  

