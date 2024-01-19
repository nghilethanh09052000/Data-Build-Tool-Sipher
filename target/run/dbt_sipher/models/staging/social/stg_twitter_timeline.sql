

   

      -- generated script to merge partitions into `sipher-data-testing`.`staging_social`.`twitter_timeline`
      declare dbt_partitions_for_replacement array<timestamp>;

      
      
        

        -- 1. create a temp table
        
  
    

    create or replace table `sipher-data-testing`.`staging_social`.`twitter_timeline__dbt_tmp`
    partition by timestamp_trunc(created_at, day)
    
    OPTIONS(
      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)
    )
    as (
      WITH data AS (
    SELECT *
    FROM `sipher-data-testing`.`raw_social`.`twitter_timeline`
    WHERE `snapshot_date` = '2023-12-25'
    QUALIFY __collected_ts = MAX(__collected_ts) OVER (PARTITION BY account, created_at)
)


SELECT
  created_at,
  author_id,
  id,
  text,

  referenced_tweets,
  in_reply_to_user_id,
  reply_settings,
  conversation_id,
  lang,
  entities.annotations,
  entities.urls,
  attachments.media_keys,
  context_annotations,

  public_metrics.impression_count,
  public_metrics.like_count,
  public_metrics.reply_count,
  public_metrics.retweet_count,
  public_metrics.quote_count,

  __collected_ts,
  snapshot_date

FROM data
    );
  
      

      -- 2. define partitions to update
      set (dbt_partitions_for_replacement) = (
          select as struct
              array_agg(distinct timestamp_trunc(created_at, day))
          from `sipher-data-testing`.`staging_social`.`twitter_timeline__dbt_tmp`
      );

      -- 3. run the merge statement
      

    merge into `sipher-data-testing`.`staging_social`.`twitter_timeline` as DBT_INTERNAL_DEST
        using (
        select * from `sipher-data-testing`.`staging_social`.`twitter_timeline__dbt_tmp`
      ) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and timestamp_trunc(DBT_INTERNAL_DEST.created_at, day) in unnest(dbt_partitions_for_replacement) 
        then delete

    when not matched then insert
        (`created_at`, `author_id`, `id`, `text`, `referenced_tweets`, `in_reply_to_user_id`, `reply_settings`, `conversation_id`, `lang`, `annotations`, `urls`, `media_keys`, `context_annotations`, `impression_count`, `like_count`, `reply_count`, `retweet_count`, `quote_count`, `__collected_ts`, `snapshot_date`)
    values
        (`created_at`, `author_id`, `id`, `text`, `referenced_tweets`, `in_reply_to_user_id`, `reply_settings`, `conversation_id`, `lang`, `annotations`, `urls`, `media_keys`, `context_annotations`, `impression_count`, `like_count`, `reply_count`, `retweet_count`, `quote_count`, `__collected_ts`, `snapshot_date`)

;

      -- 4. clean up the temp table
      drop table if exists `sipher-data-testing`.`staging_social`.`twitter_timeline__dbt_tmp`

  


    