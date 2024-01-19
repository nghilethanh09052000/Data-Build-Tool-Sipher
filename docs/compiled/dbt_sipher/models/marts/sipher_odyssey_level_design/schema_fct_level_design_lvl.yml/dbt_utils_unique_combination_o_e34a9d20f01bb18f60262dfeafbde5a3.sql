





with validation_errors as (

    select
        user_id, session_id, level_start_level_count
    from `sipher-data-testing`.`sipher_odyssey_level_design`.`fct_level_design_lvl`
    group by user_id, session_id, level_start_level_count
    having count(*) > 1

)

select *
from validation_errors


