-- Create a model to determines how much profit is made on a given line of 
-- parts (specifically for all parts with green color in their name), broken 
-- out by supplier nation and year. 


with profit_made as (
    select n_name AS nation,
        date_part('year', o_orderdate) AS year , 
        p_name AS part_name,
        p_retailprice - ps_supplycost AS profit
    from {{ source('source_millage', 'part')}}  AS  p
    join {{source('source_millage', 'partsupp')}} AS ps
    ON p.p_partkey = ps.ps_partkey 
    join {{source('source_millage', 'lineitem')}} AS l
    ON ps.ps_suppkey = l.l_suppkey
    join {{source('source_millage', 'orders')}} AS o
    ON l.l_orderkey = o.o_orderkey
    join {{source('source_millage', 'customer')}} AS c
    ON o.o_custkey = c.c_custkey    
    join {{source('source_millage', 'nation')}} AS n
    ON c.c_nationkey = n.n_nationkey
    ),

Profit_made_on_green_parts as (
    select nation,
        year, 
        SUM(profit) AS total_profit
    from profit_made
    where lower(part_name) LIKE '%green%'
    Group by 1,2
    order by 3 desc    
)
select * from profit_made_on_green_parts


