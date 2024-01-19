

with 

dungeon_id_difficulty as (
  SELECT distinct dungeon_id_difficulty ,
  dense_rank() over(order by dungeon_id_difficulty ) as stt
FROM `sipher-data-testing`.`sipher_odyssey_level_design`.`mart_level_design_gameplay`
WHERE true
and REGEXP_CONTAINS(UPPER(dungeon_id),  'FTUE|ENDLESS') = FALSE 
)

,gameplaytb AS (
SELECT
    DISTINCT g1.user_id,
    g1.dungeon_id_difficulty,
    gameplay_status,
    stt
FROM
    `sipher-data-testing`.`sipher_odyssey_level_design`.`mart_level_design_gameplay` g1
JOIN dungeon_id_difficulty dd using(dungeon_id_difficulty)
WHERE TRUE
    AND UPPER(dungeon_id) NOT LIKE '%FTUE%' 
    AND UPPER(dungeon_id) NOT LIKE '%ENDLESS%' 
    
    )



SELECT
cur_dungeon.*
FROM
gameplaytb cur_dungeon
LEFT JOIN
gameplaytb pre_dungeon
ON
cur_dungeon.user_id = pre_dungeon.user_id
AND cur_dungeon.stt = pre_dungeon.stt + 1
AND pre_dungeon.gameplay_status = 'SUCCESS'
WHERE
TRUE
AND cur_dungeon.stt > 1
AND pre_dungeon.gameplay_status IS NULL

