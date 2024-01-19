







with validation as (
  select
    
    sum(case when act_date is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion
  from `sipher-data-testing`.`tmp_dbt`.`SIPHER_overall`
  
),
validation_errors as (
  select
    
    not_null_proportion
  from validation
  where not_null_proportion < 0.8 or not_null_proportion > 1
)
select
  *
from validation_errors

