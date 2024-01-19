







with validation as (
  select
    
    sum(case when period is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion
  from `sipher-data-testing`.`tmp_dbt`.`quest_retention_weekly_hd`
  
),
validation_errors as (
  select
    
    not_null_proportion
  from validation
  where not_null_proportion < 0.15 or not_null_proportion > 1
)
select
  *
from validation_errors

