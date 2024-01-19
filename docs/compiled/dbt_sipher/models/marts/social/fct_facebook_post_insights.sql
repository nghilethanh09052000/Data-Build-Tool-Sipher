WITH posts AS (
    SELECT
        post_id
        , message
        , post_url
        , status_type
        , privacy_description
        , created_time
        , latest_update_at
    FROM
        `sipher-data-testing`.`mart_social`.`dim_facebook_post`
)
, post_shares AS (
    SELECT
        post_id
        , MAX(COALESCE(shares, 0)) AS shares
        , snapshot_date
    FROM
        `sipher-data-testing`.`staging_social`.`stg_facebook_page_feed`
    GROUP BY 1,3

)
, comments AS (
    SELECT
        post_id
        , COUNT(DISTINCT message_id) AS comments
        , snapshot_date
    FROM
        `sipher-data-testing`.`staging_social`.`stg_facebook_post_comments`
    GROUP BY 1,3
)
, post_ins AS (
    SELECT
        post_id
        , period
        ,COALESCE(post_clicks, 0) AS post_clicks,
        COALESCE(post_clicks_unique, 0) AS post_clicks_unique,
        COALESCE(post_engaged_fan, 0) AS post_engaged_fan,
        COALESCE(post_engaged_users, 0) AS post_engaged_users,
        COALESCE(post_impressions, 0) AS post_impressions,
        COALESCE(post_impressions_fan, 0) AS post_impressions_fan,
        COALESCE(post_impressions_fan_paid, 0) AS post_impressions_fan_paid,
        COALESCE(post_impressions_fan_paid_unique, 0) AS post_impressions_fan_paid_unique,
        COALESCE(post_impressions_fan_unique, 0) AS post_impressions_fan_unique,
        COALESCE(post_impressions_nonviral, 0) AS post_impressions_nonviral,
        COALESCE(post_impressions_nonviral_unique, 0) AS post_impressions_nonviral_unique,
        COALESCE(post_impressions_organic, 0) AS post_impressions_organic,
        COALESCE(post_impressions_organic_unique, 0) AS post_impressions_organic_unique,
        COALESCE(post_impressions_paid, 0) AS post_impressions_paid,
        COALESCE(post_impressions_paid_unique, 0) AS post_impressions_paid_unique,
        COALESCE(post_impressions_unique, 0) AS post_impressions_unique,
        COALESCE(post_impressions_viral, 0) AS post_impressions_viral,
        COALESCE(post_impressions_viral_unique, 0) AS post_impressions_viral_unique,
        COALESCE(post_negative_feedback, 0) AS post_negative_feedback,
        COALESCE(post_negative_feedback_unique, 0) AS post_negative_feedback_unique,
        COALESCE(post_reactions_anger_total, 0) AS post_reactions_anger_total,
        COALESCE(post_reactions_haha_total, 0) AS post_reactions_haha_total,
        COALESCE(post_reactions_like_total, 0) AS post_reactions_like_total,
        COALESCE(post_reactions_love_total, 0) AS post_reactions_love_total,
        COALESCE(post_reactions_sorry_total, 0) AS post_reactions_sorry_total,
        COALESCE(post_reactions_wow_total, 0) AS post_reactions_wow_total,
        snapshot_date
    FROM 
        `sipher-data-testing`.`staging_social`.`stg_facebook_post_insights`
)

, final AS (
    SELECT
        pi.snapshot_date AS date_tzutc
        , pi.post_id
        , p.message
        , p.post_url
        , p.status_type
        , p.privacy_description
        , p.created_time AS post_created_at
        , p.latest_update_at AS post_latest_update_at
        , period
        , COALESCE(ps.shares, 0) AS shares
        , (post_reactions_anger_total 
            + post_reactions_haha_total 
            + post_reactions_like_total
            + post_reactions_love_total
            + post_reactions_sorry_total
            + post_reactions_wow_total
        ) AS reactions
        , COALESCE(c.comments, 0) AS comments
        , (post_reactions_like_total
            + post_reactions_love_total
            + post_reactions_wow_total
            + post_reactions_haha_total
            + post_reactions_sorry_total
            + post_reactions_anger_total
            + post_clicks
            + COALESCE(c.comments, 0)
            + COALESCE(ps.shares, 0)
        ) AS post_engagement

        , post_engaged_fan
        , post_engaged_users
        
        , post_impressions
        , post_impressions_unique AS post_reach
        , post_impressions_organic_unique
        , post_impressions_paid_unique
        , post_impressions_organic
        , post_impressions_paid
        
        , post_impressions_fan
        , post_impressions_fan_paid
        , post_impressions_fan_paid_unique
        , post_impressions_fan_unique

        , post_impressions_nonviral
        , post_impressions_nonviral_unique
        
        , post_impressions_viral
        , post_impressions_viral_unique

        , post_negative_feedback
        , post_negative_feedback_unique

        , post_reactions_anger_total
        , post_reactions_haha_total
        , post_reactions_like_total
        , post_reactions_love_total
        , post_reactions_sorry_total
        , post_reactions_wow_total

        , post_clicks
        , post_clicks_unique
        
    FROM 
        post_ins AS pi
    LEFT JOIN posts p ON pi.post_id = p.post_id
    LEFT JOIN post_shares ps ON pi.post_id = ps.post_id AND pi.snapshot_date = ps.snapshot_date
    LEFT JOIN comments c ON pi.post_id = c.post_id AND pi.snapshot_date = c.snapshot_date

    ORDER BY pi.snapshot_date
)

SELECT * FROM final