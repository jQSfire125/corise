/*  Project 1 - Exercise 1
    The customers cte gest the data from customer_data and customer_address
    to get the required fields. It also standardizes the format of city and state.

    The cities cte chooses only one row per city-state pair arbitrarily and standardizes
    the format of city and state.

    The customers_geo cte adds the geo_location from cities to the customers data.

    The suppliers table standardizes the city and state and gets the geo_location data 
    as well.

    The final table uses cross join to match all 10 suppliers with each customer and
    chooses the supplier that is closest (based on geo_location). It also filters
    for customers for which we don't have geo_location data. */

with customers as (
    select 
        data.customer_id
        ,   data.first_name
        ,   data.last_name
        ,   data.email
        ,   initcap(trim(address.customer_city)) as customer_city
        ,   upper(trim(address.customer_state)) as customer_state
    from vk_data.customers.customer_data as data
    left join vk_data.customers.customer_address as address using(customer_id)
),

cities as (
    select
        initcap(trim(city_name)) as city_name
        ,   upper(trim(state_abbr)) as state_abbr
        ,   geo_location
    from vk_data.resources.us_cities
    qualify row_number() over(partition by city_name, state_abbr order by city_name) = 1
),

customers_geo as (
    select
        customers.customer_id
        ,   customers.first_name
        ,   customers.last_name
        ,   customers.email
        ,   customers.customer_city
        ,   customers.customer_state
        ,   cities.geo_location
    from customers
    left join cities
        on (customers.customer_city = cities.city_name
           and customers.customer_state = cities.state_abbr)
),

suppliers as (
    select
        supplier_info.supplier_id
        ,   supplier_info.supplier_name
        ,   initcap(trim(supplier_info.supplier_city)) as supplier_city
        ,   upper(trim(supplier_state)) as supplier_state
        ,   cities.geo_location
    from vk_data.suppliers.supplier_info
    left join cities
            on (supplier_info.supplier_city = cities.city_name
               and supplier_info.supplier_state = cities.state_abbr)
),

final as (
    select
        customers_geo.customer_id
        ,   customers_geo.first_name
        ,   customers_geo.last_name
        ,   customers_geo.email
        ,   suppliers.supplier_id
        ,   suppliers.supplier_name
        ,   st_distance(customers_geo.geo_location, suppliers.geo_location)/1000 as distance_in_kms
    from customers_geo
    cross join suppliers
    where customers_geo.geo_location is not null 
    qualify row_number() over(partition by customers_geo.customer_id order by distance_in_kms) = 1
    order by customers_geo.last_name, customers_geo.first_name
)

select * from final;
