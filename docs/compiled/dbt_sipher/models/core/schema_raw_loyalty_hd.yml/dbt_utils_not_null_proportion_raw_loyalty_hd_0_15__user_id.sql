







with validation as (
  select
    
    sum(case when user_id is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion
  from `sipher-data-testing`.`tmp_dbt`.`raw_loyalty_hd`
  
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

