/* General Commments:
    I am trying to follow the sample output provided.
    That means using part price to rank top orders 
    and having total spent be the sum of those 3 parts.
    
    I believe the correct way would be to use order total for
    ranking and total spent. */

with 
    automobile_customers as (
        select
            c_custkey
        from snowflake_sample_data.tpch_sf1.customer
        where c_mktsegment = 'AUTOMOBILE' --automobile customers only
),

    urgent_orders as (
        select
            o_orderkey
            ,   o_custkey
            ,   o_orderdate
        from snowflake_sample_data.tpch_sf1.orders
        where o_orderpriority = '1-URGENT' --only urgent orders
),

    orders_line_item as (
        select 
            automobile_customers.c_custkey
            ,   urgent_orders.o_orderdate
            ,   urgent_orders.o_orderkey
            ,   lineitem.l_partkey
            ,   lineitem.l_quantity
            ,   lineitem.l_extendedprice
            ,   row_number () over (partition by c_custkey order by l_extendedprice desc) as price_rank --most expensive parts
        from automobile_customers
        inner join urgent_orders 
            on automobile_customers.c_custkey = urgent_orders.o_custkey
        inner join snowflake_sample_data.tpch_sf1.lineitem 
            on urgent_orders.o_orderkey = lineitem.l_orderkey
        qualify price_rank <= 3 --only the 3 most expensive parts overall
),

    results as (
        select
            c_custkey
            ,   max(o_orderdate) as last_order_date
            ,   listagg(o_orderkey, ', ') as order_numbers
            ,   sum(l_extendedprice) as total_spent --total spent on those 3 most expensive parts
            ,   max(case when price_rank = 1 then l_partkey end) as part_1_key
            ,   max(case when price_rank = 1 then l_quantity end) as part_1_quantity
            ,   max(case when price_rank = 1 then l_extendedprice end) as part_1_total_spent
            ,   max(case when price_rank = 2 then l_partkey end) as part_2_key
            ,   max(case when price_rank = 2 then l_quantity end) as part_2_quantity
            ,   max(case when price_rank = 2 then l_extendedprice end) as part_2_total_spent
            ,   max(case when price_rank = 3 then l_partkey end) as part_3_key
            ,   max(case when price_rank = 3 then l_quantity end) as part_3_quantity
            ,   max(case when price_rank = 3 then l_extendedprice end) as part_3_total_spent
        from orders_line_item
        group by c_custkey
)

select * from results
order by last_order_date desc
limit 100

/* My feedback on the candidate's work is here: 
https://github.com/jQSfire125/corise/blob/main/AdvancedSQL/project_4_exercise_2.md */