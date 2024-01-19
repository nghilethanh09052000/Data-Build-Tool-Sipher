
with exceptions as (
    SELECT distinct session_id, user_id, dungeon_id_difficulty 
    FROM`sipher-data-testing`.`sipher_odyssey_level_design`.`mart_level_design_gameplay`
    WHERE true
    AND  REGEXP_CONTAINS(dungeon_id_difficulty, r'DUNGEON') 
    AND NOT REGEXP_CONTAINS(dungeon_id_difficulty, r'^\d{2}')  
    AND NOT REGEXP_CONTAINS(dungeon_id_difficulty, r'\d{2}$')  
)

select * from exceptions

