



select
    *
from (select * from `sipher-data-testing`.`sipher_odyssey_level_design`.`fct_level_design_lvl` where  REGEXP_CONTAINS(UPPER(dungeon_id),  'FTUE|ENDLESS') = FALSE ) dbt_subquery

where not(level_start_level_count <= 12)

