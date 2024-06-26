SELECT
  *,
  _TABLE_SUFFIX AS __table_suffix
FROM 
  `art-venture-387704`.`analytics_376091252`.`events_*`
WHERE 
    PARSE_DATE('%Y%m%d', SUBSTR(_TABLE_SUFFIX, -8)) BETWEEN DATE_SUB(CURRENT_DATE() , INTERVAL 14 DAY) AND CURRENT_DATE()