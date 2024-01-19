



select
    *
from `sipher-data-testing`.`tmp_dbt`.`quest_4retention_daily_hd`

where not(retention  >= 0)

