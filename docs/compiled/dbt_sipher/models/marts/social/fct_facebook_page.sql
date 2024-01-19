WITH pg_over AS (
    SELECT
        page_id
        , page_name
        , username
        , followers_count
        , category
        , link
        , snapshot_date
    FROM
       `sipher-data-testing`.`staging_social`.`stg_facebook_page_overall` 
)
,page_ins AS (
    SELECT
        CAST(end_time AS DATE) AS end_time_insights
        , end_time
        , page_id
        , period
        , 
            COALESCE(page_actions_post_reactions_like_total, 0) AS page_actions_post_reactions_like_total,
        
            COALESCE(page_actions_post_reactions_love_total, 0) AS page_actions_post_reactions_love_total,
        
            COALESCE(page_actions_post_reactions_wow_total, 0) AS page_actions_post_reactions_wow_total,
        
            COALESCE(page_actions_post_reactions_haha_total, 0) AS page_actions_post_reactions_haha_total,
        
            COALESCE(page_actions_post_reactions_sorry_total, 0) AS page_actions_post_reactions_sorry_total,
        
            COALESCE(page_actions_post_reactions_anger_total, 0) AS page_actions_post_reactions_anger_total,
        
            COALESCE(page_consumptions, 0) AS page_consumptions,
        
            COALESCE(page_consumptions_unique, 0) AS page_consumptions_unique,
        
            COALESCE(page_consumptions_by_other_clicks, 0) AS page_consumptions_by_other_clicks,
        
            COALESCE(page_consumptions_by_other_clicks_unique, 0) AS page_consumptions_by_other_clicks_unique,
        
            COALESCE(page_consumptions_by_photo_view, 0) AS page_consumptions_by_photo_view,
        
            COALESCE(page_consumptions_by_photo_view_unique, 0) AS page_consumptions_by_photo_view_unique,
        
            COALESCE(page_content_activity, 0) AS page_content_activity,
        
            COALESCE(page_content_activity_by_fan, 0) AS page_content_activity_by_fan,
        
            COALESCE(page_content_activity_by_fan_unique, 0) AS page_content_activity_by_fan_unique,
        
            COALESCE(page_content_activity_by_page_post, 0) AS page_content_activity_by_page_post,
        
            COALESCE(page_content_activity_by_page_post_unique, 0) AS page_content_activity_by_page_post_unique,
        
            COALESCE(page_content_activity_by_other, 0) AS page_content_activity_by_other,
        
            COALESCE(page_content_activity_by_other_unique, 0) AS page_content_activity_by_other_unique,
        
            COALESCE(page_engaged_users, 0) AS page_engaged_users,
        
            COALESCE(page_fans, 0) AS page_fans,
        
            COALESCE(page_fan_adds_total, 0) AS page_fan_adds_total,
        
            COALESCE(page_fan_adds_paid, 0) AS page_fan_adds_paid,
        
            COALESCE(page_fan_adds_unpaid, 0) AS page_fan_adds_unpaid,
        
            COALESCE(page_fan_removes, 0) AS page_fan_removes,
        
            COALESCE(page_fan_removes_unique, 0) AS page_fan_removes_unique,
        
            COALESCE(page_impressions, 0) AS page_impressions,
        
            COALESCE(page_impressions_by_page_post, 0) AS page_impressions_by_page_post,
        
            COALESCE(page_impressions_by_page_post_unique, 0) AS page_impressions_by_page_post_unique,
        
            COALESCE(page_impressions_by_checkin, 0) AS page_impressions_by_checkin,
        
            COALESCE(page_impressions_by_checkin_unique, 0) AS page_impressions_by_checkin_unique,
        
            COALESCE(page_impressions_nonviral, 0) AS page_impressions_nonviral,
        
            COALESCE(page_impressions_nonviral_unique, 0) AS page_impressions_nonviral_unique,
        
            COALESCE(page_impressions_organic_unique_v2, 0) AS page_impressions_organic_unique_v2,
        
            COALESCE(page_impressions_organic_v2, 0) AS page_impressions_organic_v2,
        
            COALESCE(page_impressions_paid, 0) AS page_impressions_paid,
        
            COALESCE(page_impressions_paid_unique, 0) AS page_impressions_paid_unique,
        
            COALESCE(page_impressions_unique, 0) AS page_impressions_unique,
        
            COALESCE(page_impressions_viral, 0) AS page_impressions_viral,
        
            COALESCE(page_impressions_viral_unique, 0) AS page_impressions_viral_unique,
        
            COALESCE(page_negative_feedback, 0) AS page_negative_feedback,
        
            COALESCE(page_places_checkin_total, 0) AS page_places_checkin_total,
        
            COALESCE(page_places_checkin_total_unique, 0) AS page_places_checkin_total_unique,
        
            COALESCE(page_places_checkin_mobile, 0) AS page_places_checkin_mobile,
        
            COALESCE(page_places_checkin_mobile_unique, 0) AS page_places_checkin_mobile_unique,
        
            COALESCE(page_positive_feedback_by_link, 0) AS page_positive_feedback_by_link,
        
            COALESCE(page_positive_feedback_by_link_unique, 0) AS page_positive_feedback_by_link_unique,
        
            COALESCE(page_positive_feedback_by_like, 0) AS page_positive_feedback_by_like,
        
            COALESCE(page_positive_feedback_by_like_unique, 0) AS page_positive_feedback_by_like_unique,
        
            COALESCE(page_positive_feedback_by_comment, 0) AS page_positive_feedback_by_comment,
        
            COALESCE(page_positive_feedback_by_comment_unique, 0) AS page_positive_feedback_by_comment_unique,
        
            COALESCE(page_positive_feedback_by_other, 0) AS page_positive_feedback_by_other,
        
            COALESCE(page_positive_feedback_by_other_unique, 0) AS page_positive_feedback_by_other_unique,
        
            COALESCE(page_post_engagements, 0) AS page_post_engagements,
        
            COALESCE(page_posts_impressions, 0) AS page_posts_impressions,
        
            COALESCE(page_posts_impressions_nonviral, 0) AS page_posts_impressions_nonviral,
        
            COALESCE(page_posts_impressions_nonviral_unique, 0) AS page_posts_impressions_nonviral_unique,
        
            COALESCE(page_posts_impressions_organic, 0) AS page_posts_impressions_organic,
        
            COALESCE(page_posts_impressions_organic_unique, 0) AS page_posts_impressions_organic_unique,
        
            COALESCE(page_posts_impressions_paid, 0) AS page_posts_impressions_paid,
        
            COALESCE(page_posts_impressions_paid_unique, 0) AS page_posts_impressions_paid_unique,
        
            COALESCE(page_posts_impressions_unique, 0) AS page_posts_impressions_unique,
        
            COALESCE(page_posts_impressions_viral, 0) AS page_posts_impressions_viral,
        
            COALESCE(page_posts_impressions_viral_unique, 0) AS page_posts_impressions_viral_unique,
        
            COALESCE(page_posts_served_impressions_organic_unique, 0) AS page_posts_served_impressions_organic_unique,
        
            COALESCE(page_video_complete_views_30s, 0) AS page_video_complete_views_30s,
        
            COALESCE(page_video_complete_views_30s_autoplayed, 0) AS page_video_complete_views_30s_autoplayed,
        
            COALESCE(page_video_complete_views_30s_click_to_play, 0) AS page_video_complete_views_30s_click_to_play,
        
            COALESCE(page_video_complete_views_30s_organic, 0) AS page_video_complete_views_30s_organic,
        
            COALESCE(page_video_complete_views_30s_paid, 0) AS page_video_complete_views_30s_paid,
        
            COALESCE(page_video_complete_views_30s_repeat_views, 0) AS page_video_complete_views_30s_repeat_views,
        
            COALESCE(page_video_complete_views_30s_unique, 0) AS page_video_complete_views_30s_unique,
        
            COALESCE(page_video_repeat_views, 0) AS page_video_repeat_views,
        
            COALESCE(page_video_view_time, 0) AS page_video_view_time,
        
            COALESCE(page_video_views, 0) AS page_video_views,
        
            COALESCE(page_video_views_10s, 0) AS page_video_views_10s,
        
            COALESCE(page_video_views_10s_autoplayed, 0) AS page_video_views_10s_autoplayed,
        
            COALESCE(page_video_views_10s_click_to_play, 0) AS page_video_views_10s_click_to_play,
        
            COALESCE(page_video_views_10s_organic, 0) AS page_video_views_10s_organic,
        
            COALESCE(page_video_views_10s_paid, 0) AS page_video_views_10s_paid,
        
            COALESCE(page_video_views_10s_repeat, 0) AS page_video_views_10s_repeat,
        
            COALESCE(page_video_views_10s_unique, 0) AS page_video_views_10s_unique,
        
            COALESCE(page_video_views_autoplayed, 0) AS page_video_views_autoplayed,
        
            COALESCE(page_video_views_by_page_uploaded, 0) AS page_video_views_by_page_uploaded,
        
            COALESCE(page_video_views_by_page_uploaded_from_crossposts, 0) AS page_video_views_by_page_uploaded_from_crossposts,
        
            COALESCE(page_video_views_by_page_uploaded_from_shares, 0) AS page_video_views_by_page_uploaded_from_shares,
        
            COALESCE(page_video_views_by_page_hosted_crosspost, 0) AS page_video_views_by_page_hosted_crosspost,
        
            COALESCE(page_video_views_by_page_hosted_share, 0) AS page_video_views_by_page_hosted_share,
        
            COALESCE(page_video_views_by_page_owned, 0) AS page_video_views_by_page_owned,
        
            COALESCE(page_video_views_click_to_play, 0) AS page_video_views_click_to_play,
        
            COALESCE(page_video_views_organic, 0) AS page_video_views_organic,
        
            COALESCE(page_video_views_paid, 0) AS page_video_views_paid,
        
            COALESCE(page_video_views_unique, 0) AS page_video_views_unique,
        
            COALESCE(page_views_logged_in_total, 0) AS page_views_logged_in_total,
        
            COALESCE(page_views_logged_in_unique, 0) AS page_views_logged_in_unique,
        
            COALESCE(page_views_total, 0) AS page_views_total,
        
            COALESCE(page_views_by_internal_referer_logged_in_unique_other, 0) AS page_views_by_internal_referer_logged_in_unique_other,
        
            COALESCE(page_views_by_internal_referer_logged_in_unique_none, 0) AS page_views_by_internal_referer_logged_in_unique_none,
        
            COALESCE(page_views_by_internal_referer_logged_in_unique_search, 0) AS page_views_by_internal_referer_logged_in_unique_search,
        
            COALESCE(page_views_by_profile_tab_logged_in_unique_photos, 0) AS page_views_by_profile_tab_logged_in_unique_photos,
        
            COALESCE(page_views_by_profile_tab_logged_in_unique_home, 0) AS page_views_by_profile_tab_logged_in_unique_home,
        
            COALESCE(page_views_by_profile_tab_logged_in_unique_about, 0) AS page_views_by_profile_tab_logged_in_unique_about,
        
            COALESCE(page_views_by_profile_tab_total_photos, 0) AS page_views_by_profile_tab_total_photos,
        
            COALESCE(page_views_by_profile_tab_total_home, 0) AS page_views_by_profile_tab_total_home,
        
            COALESCE(page_views_by_profile_tab_total_about, 0) AS page_views_by_profile_tab_total_about,
        
            COALESCE(page_views_by_site_logged_in_unique_other, 0) AS page_views_by_site_logged_in_unique_other,
        
            COALESCE(page_views_by_site_logged_in_unique_mobile, 0) AS page_views_by_site_logged_in_unique_mobile,
        
            COALESCE(page_views_by_site_logged_in_unique_www, 0) AS page_views_by_site_logged_in_unique_www,
        snapshot_date
    FROM
        `sipher-data-testing`.`staging_social`.`stg_facebook_page_insights` 
)
, final AS (
    SELECT
        pi.end_time_insights
        , po.page_id
        , po.page_name
        , po.username
        , po.followers_count
        , po.category
        , po.link
        , 
            pi.page_actions_post_reactions_like_total,
        
            pi.page_actions_post_reactions_love_total,
        
            pi.page_actions_post_reactions_wow_total,
        
            pi.page_actions_post_reactions_haha_total,
        
            pi.page_actions_post_reactions_sorry_total,
        
            pi.page_actions_post_reactions_anger_total,
        
            pi.page_consumptions,
        
            pi.page_consumptions_unique,
        
            pi.page_consumptions_by_other_clicks,
        
            pi.page_consumptions_by_other_clicks_unique,
        
            pi.page_consumptions_by_photo_view,
        
            pi.page_consumptions_by_photo_view_unique,
        
            pi.page_content_activity,
        
            pi.page_content_activity_by_fan,
        
            pi.page_content_activity_by_fan_unique,
        
            pi.page_content_activity_by_page_post,
        
            pi.page_content_activity_by_page_post_unique,
        
            pi.page_content_activity_by_other,
        
            pi.page_content_activity_by_other_unique,
        
            pi.page_engaged_users,
        
            pi.page_fans,
        
            pi.page_fan_adds_total,
        
            pi.page_fan_adds_paid,
        
            pi.page_fan_adds_unpaid,
        
            pi.page_fan_removes,
        
            pi.page_fan_removes_unique,
        
            pi.page_impressions,
        
            pi.page_impressions_by_page_post,
        
            pi.page_impressions_by_page_post_unique,
        
            pi.page_impressions_by_checkin,
        
            pi.page_impressions_by_checkin_unique,
        
            pi.page_impressions_nonviral,
        
            pi.page_impressions_nonviral_unique,
        
            pi.page_impressions_organic_unique_v2,
        
            pi.page_impressions_organic_v2,
        
            pi.page_impressions_paid,
        
            pi.page_impressions_paid_unique,
        
            pi.page_impressions_unique,
        
            pi.page_impressions_viral,
        
            pi.page_impressions_viral_unique,
        
            pi.page_negative_feedback,
        
            pi.page_places_checkin_total,
        
            pi.page_places_checkin_total_unique,
        
            pi.page_places_checkin_mobile,
        
            pi.page_places_checkin_mobile_unique,
        
            pi.page_positive_feedback_by_link,
        
            pi.page_positive_feedback_by_link_unique,
        
            pi.page_positive_feedback_by_like,
        
            pi.page_positive_feedback_by_like_unique,
        
            pi.page_positive_feedback_by_comment,
        
            pi.page_positive_feedback_by_comment_unique,
        
            pi.page_positive_feedback_by_other,
        
            pi.page_positive_feedback_by_other_unique,
        
            pi.page_post_engagements,
        
            pi.page_posts_impressions,
        
            pi.page_posts_impressions_nonviral,
        
            pi.page_posts_impressions_nonviral_unique,
        
            pi.page_posts_impressions_organic,
        
            pi.page_posts_impressions_organic_unique,
        
            pi.page_posts_impressions_paid,
        
            pi.page_posts_impressions_paid_unique,
        
            pi.page_posts_impressions_unique,
        
            pi.page_posts_impressions_viral,
        
            pi.page_posts_impressions_viral_unique,
        
            pi.page_posts_served_impressions_organic_unique,
        
            pi.page_video_complete_views_30s,
        
            pi.page_video_complete_views_30s_autoplayed,
        
            pi.page_video_complete_views_30s_click_to_play,
        
            pi.page_video_complete_views_30s_organic,
        
            pi.page_video_complete_views_30s_paid,
        
            pi.page_video_complete_views_30s_repeat_views,
        
            pi.page_video_complete_views_30s_unique,
        
            pi.page_video_repeat_views,
        
            pi.page_video_view_time,
        
            pi.page_video_views,
        
            pi.page_video_views_10s,
        
            pi.page_video_views_10s_autoplayed,
        
            pi.page_video_views_10s_click_to_play,
        
            pi.page_video_views_10s_organic,
        
            pi.page_video_views_10s_paid,
        
            pi.page_video_views_10s_repeat,
        
            pi.page_video_views_10s_unique,
        
            pi.page_video_views_autoplayed,
        
            pi.page_video_views_by_page_uploaded,
        
            pi.page_video_views_by_page_uploaded_from_crossposts,
        
            pi.page_video_views_by_page_uploaded_from_shares,
        
            pi.page_video_views_by_page_hosted_crosspost,
        
            pi.page_video_views_by_page_hosted_share,
        
            pi.page_video_views_by_page_owned,
        
            pi.page_video_views_click_to_play,
        
            pi.page_video_views_organic,
        
            pi.page_video_views_paid,
        
            pi.page_video_views_unique,
        
            pi.page_views_logged_in_total,
        
            pi.page_views_logged_in_unique,
        
            pi.page_views_total,
        
            pi.page_views_by_internal_referer_logged_in_unique_other,
        
            pi.page_views_by_internal_referer_logged_in_unique_none,
        
            pi.page_views_by_internal_referer_logged_in_unique_search,
        
            pi.page_views_by_profile_tab_logged_in_unique_photos,
        
            pi.page_views_by_profile_tab_logged_in_unique_home,
        
            pi.page_views_by_profile_tab_logged_in_unique_about,
        
            pi.page_views_by_profile_tab_total_photos,
        
            pi.page_views_by_profile_tab_total_home,
        
            pi.page_views_by_profile_tab_total_about,
        
            pi.page_views_by_site_logged_in_unique_other,
        
            pi.page_views_by_site_logged_in_unique_mobile,
        
            pi.page_views_by_site_logged_in_unique_www,
        FROM
        page_ins pi
    LEFT JOIN pg_over po ON pi.end_time_insights = po.snapshot_date AND pi.page_id = po.page_id
)

SELECT * FROM final