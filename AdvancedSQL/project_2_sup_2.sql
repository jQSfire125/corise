with 
    cities as (
        select
            initcap(trim(city_name)) as city_name
            ,   upper(trim(state_abbr)) as state_abbr
            ,   geo_location
        from vk_data.resources.us_cities
        qualify row_number() over(partition by city_name, state_abbr order by city_name) = 1
),
    
    suppliers as (
        select
            supplier_info.supplier_id
            ,   supplier_info.supplier_name
            ,   supplier_city || ', ' || supplier_state as supplier_location
            ,   initcap(trim(supplier_info.supplier_city)) as supplier_city
            ,   upper(trim(supplier_state)) as supplier_state
            ,   cities.geo_location
        from vk_data.suppliers.supplier_info
        left join cities
                on (supplier_info.supplier_city = cities.city_name
                and supplier_info.supplier_state = cities.state_abbr)
)

select
    suppliers_1.supplier_id
    ,   suppliers_1.supplier_name
    ,   suppliers_1.supplier_location as location_main
    ,   suppliers_2.supplier_location as location_backup
    ,   round(st_distance(suppliers_1.geo_location, 
                          suppliers_2.geo_location)/1609) as travel_miles
from suppliers as suppliers_1
cross join suppliers as suppliers_2
where suppliers_1.supplier_id != suppliers_2.supplier_id
qualify row_number() over(partition by suppliers_1.supplier_id order by travel_miles) = 1
order by supplier_name;