/*  Use leading commas (easier to add or remove lines later)

    Use lower case (personal preference, easier to read)
    
    Use table names instead of aliases where table names are 2 words or less
    (easier to understand when reading the query)
    
    Use column names in group by to make it easier to read 
    
    Prefix column names with table names/alias (easier to remember what
    comes from where) 
    
    Standardize functions (trim vs rtrim + ltrim)
      
    Make type join explicit (inner join vs join)
    
    Move subqueries to CTEs to make the code easier to understand and also 
    easier to adapt later
    
    In join conditions, put the first table after ON */

with
    clean_us_cities as (
        select
            initcap(trim(city_name)) as city_name
            ,   upper(trim(state_abbr)) as state_abbr
            ,   geo_location
        from vk_data.resources.us_cities   
    ),    
    
    affected_customers as (
        select
            customer_data.customer_id
            ,   customer_data.first_name || ' ' || customer_data.last_name as customer_name
            ,   initcap(trim(customer_address.customer_city)) as customer_city
            ,   upper(trim(customer_address.customer_state)) as customer_state
            ,   us_cities.geo_location
        from vk_data.customers.customer_data
        inner join vk_data.customers.customer_address
            on customer_data.customer_id = customer_address.customer_id
        left join clean_us_cities as us_cities
            on upper(trim(customer_address.customer_state)) = us_cities.state_abbr
                and initcap(trim(customer_address.customer_city)) = us_cities.city_name
        where 
            ((us_cities.city_name ilike '%concord%' 
                or us_cities.city_name ilike '%georgetown%' 
                or us_cities.city_name ilike '%ashland%') 
                and customer_address.customer_state = 'KY')
            or ((us_cities.city_name ilike '%oakland%' 
                or us_cities.city_name ilike '%pleasant hill%')
                and customer_address.customer_state = 'CA')
            or ((customer_address.customer_state = 'TX' 
                and us_cities.city_name ilike '%arlington%') 
                or us_cities.city_name ilike '%brownsville%')
),

    active_customer_preferences as (
        select 
            customer_id
            ,   count(*) as food_pref_count
        from vk_data.customers.customer_survey
        where is_active = true
        group by customer_id
),

    chicago_store as ( 
        select 
            geo_location
        from vk_data.resources.us_cities 
        where 
            city_name = 'CHICAGO' 
            and state_abbr = 'IL'
),

    gary_store as (
        select 
            geo_location
        from vk_data.resources.us_cities 
        where 
            city_name = 'GARY' 
            and state_abbr = 'IN'
)

select 
    affected_customers.customer_name
    ,   affected_customers.customer_city
    ,   affected_customers.customer_state
    ,   active_customer_preferences.food_pref_count
    ,   (st_distance(affected_customers.geo_location, chicago_store.geo_location) / 1609)::int as chicago_distance_miles
    ,   (st_distance(affected_customers.geo_location, gary_store.geo_location) / 1609)::int as gary_distance_miles
from affected_customers
inner join active_customer_preferences on affected_customers.customer_id = active_customer_preferences.customer_id
cross join chicago_store
cross join gary_store;