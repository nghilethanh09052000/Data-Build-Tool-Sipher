

with meet_condition as(
  select *
  from `sipher-data-testing`.`tmp_dbt`.`atherlabs_dashboard_MAU`
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not user_pseudo_id_cnt >= 0
)

select *
from validation_errors

