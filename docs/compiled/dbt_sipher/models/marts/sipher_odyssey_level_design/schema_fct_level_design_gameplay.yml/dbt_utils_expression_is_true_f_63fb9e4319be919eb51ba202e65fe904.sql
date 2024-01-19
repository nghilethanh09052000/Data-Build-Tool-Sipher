



select
    *
from `sipher-data-testing`.`sipher_odyssey_level_design`.`fct_level_design_gameplay`

where not(LENGTH(session_id) >= 5)

