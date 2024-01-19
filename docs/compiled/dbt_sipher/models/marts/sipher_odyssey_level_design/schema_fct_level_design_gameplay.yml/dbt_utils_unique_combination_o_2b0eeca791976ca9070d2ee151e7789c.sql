





with validation_errors as (

    select
        user_id, session_id
    from `sipher-data-testing`.`sipher_odyssey_level_design`.`fct_level_design_gameplay`
    group by user_id, session_id
    having count(*) > 1

)

select *
from validation_errors


