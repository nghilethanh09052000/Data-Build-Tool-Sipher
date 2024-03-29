SELECT
    id,
    name,
    symbol,
    open AS open_price,
    high,
    low,
    close AS close_price,
    PARSE_DATE("%Y-%m-%d", LEFT(timestamp,10)) AS quote_date,
    timestamp,
FROM `sipher-data-platform`.`raw_coinmarketcap`.`main_token_quotes`