
with exceptions as (
      SELECT
              *        
      FROM `sipher-data-testing`.`sipher_odyssey_level_design`.`fct_level_design_lvl`
      WHERE TRUE 
        AND IFNULL(down,0) > 0 AND IFNULL(hp_loss,0) = 0
      )

select * from exceptions

