with 
    deduped_events as (
        select
            event_id
            ,   session_id
            ,   event_timestamp
            ,   trim(parse_json(event_details):event) as event_type
            ,   trim(parse_json(event_details):recipe_id) as recipe_id
        from vk_data.events.website_activity
        group by event_id,session_id,event_timestamp,event_type,recipe_id
),

    session_info as (
        select
            session_id
            ,   min(event_timestamp) as session_start
            ,   max(event_timestamp) as session_end
            ,   datediff(second, session_start, session_end) as session_length_seconds
            ,   case 
                    when count_if(event_type = 'view_recipe') = 0 then null
                    else count_if(event_type = 'search') / count_if(event_type = 'view_recipe')
                end as searches_per_recipe_view
        from deduped_events
        group by session_id
),

    most_viewed_recipe_day as (
        select
            date(event_timestamp) as day
            ,   recipe_id
            ,   count(*) as nb_viewed
        from deduped_events
        where recipe_id is not null
        group by day, recipe_id
        qualify row_number() over (partition by day order by nb_viewed desc) = 1
),

    final as (
        select 
            date(session_info.session_start) as day
            ,   count(session_info.session_id) as total_sessions
            ,   round(avg(session_info.session_length_seconds),1) as avg_session_length_seconds
            ,   avg(session_info.searches_per_recipe_view) as avg_searches_per_recipe_view
            ,   max(most_viewed_recipe_day.recipe_id) as recipe_most_viewed
        from session_info
        inner join most_viewed_recipe_day on date(session_info.session_start) = most_viewed_recipe_day.day
        group by 1
)


select * from final