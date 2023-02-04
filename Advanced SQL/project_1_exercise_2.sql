
/*
Project 2 - Exercise 2
The customers cte gest the data from customer_data and customer_address
to get the required fields. It also standardizes the format of city and state.

The cities cte chooses only one row per city-state pair arbitrarily and standardizes
the format of city and state.

In the customers_elegible cte we join to get the customers for who we have geo_location data.

In customers_survey we get the top 3 preferences (tags) alfabetically by customer. 
Then we pivot by preference in customers_survey_pivoted.

In all_recipe_suggestions we get for each recipe its tags, and in one_recipe_suggestion
we narrow it down to one recipe per tag using any_value.

The final cte gets the required fields together by pulling from customers_elegible, 
customers_survey_pivoted and one_recipe_suggestion.
*/

with customers as (
    select 
        data.customer_id
        ,   data.email
        ,   data.first_name
        ,   initcap(trim(address.customer_city)) as customer_city
        ,   upper(trim(address.customer_state)) as customer_state
    from vk_data.customers.customer_data as data
    inner join vk_data.customers.customer_address as address using(customer_id)
),

cities as (
    select
        initcap(trim(city_name)) as city_name
        ,   upper(trim(state_abbr)) as state_abbr
    from vk_data.resources.us_cities
    qualify row_number() over(partition by city_name, state_abbr order by city_name) = 1
),

customers_elegible as (
    select
        customers.customer_id
        ,   customers.email
        ,   customers.first_name
    from customers
    inner join cities
        on (customers.customer_city = cities.city_name
           and customers.customer_state = cities.state_abbr)
),

customers_survey as(
    select
        customer_survey.customer_id 
        ,   lower(trim(recipe_tags.tag_property)) as tag_property
        ,   row_number() over(partition by customer_id order by tag_property) as tag_order
    from vk_data.customers.customer_survey
    inner join vk_data.resources.recipe_tags
        using(tag_id)
    where customer_survey.is_active = true
    qualify tag_order <=3   
),

customers_survey_pivoted as (
    select *
    from customers_survey
    pivot(max(tag_property)
        for tag_order in (1,2,3))
    as pivot_values (customer_id, food_pref_1, food_pref_2, food_pref_3)
),

all_recipe_suggestions as (
    select 
        recipe_id
        ,   recipe_name
        ,   lower(trim(flat_tag_list.value::string)) as recipe_tag
    from vk_data.chefs.recipe
    ,   table(flatten(tag_list)) as flat_tag_list
),

one_recipe_suggestion as (
    select 
        recipe_tag
        , any_value(recipe_name) as suggested_recipe
    from all_recipe_suggestions
    group by recipe_tag
),

final as (
    select 
        customers_survey_pivoted.customer_id
        ,   customers_elegible.email
        ,   customers_elegible.first_name
        ,   customers_survey_pivoted.food_pref_1
        ,   customers_survey_pivoted.food_pref_2
        ,   customers_survey_pivoted.food_pref_3
        ,   one_recipe_suggestion.suggested_recipe
    from customers_survey_pivoted
    inner join customers_elegible
        on customers_survey_pivoted.customer_id = customers_elegible.customer_id     
    left join one_recipe_suggestion
        on customers_survey_pivoted.food_pref_1 = one_recipe_suggestion.recipe_tag
    order by email
)

select * from final;