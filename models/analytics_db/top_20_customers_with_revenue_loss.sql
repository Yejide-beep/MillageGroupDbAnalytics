-- Create a model that identifies the top 20 customers who might be having 
-- problems with the parts that was shipped to them within the third 
-- quarter of 1993. 
-- Your query should show the customer's name, address, nation, phone 
-- number, account balance, comment information and revenue lost. The 
-- customers should be listed in descending order of lost revenue. Revenue 
-- lost is defined as sum(l_extendedprice*(1-l_discount)) for all qualifying 
-- lineitems. 


with top_20_customers_with_revenue_loss as (
    select c_name,
        c_phone,
        c_address,
        c_acctbal,
        n_name AS nation ,
        c_comment,
        l_extendedprice - l_discount AS revenue_lost 
    from {{source('source_millage', 'lineitem')}} AS l
    join {{source('source_millage', 'orders')}} AS o
    ON l.l_orderkey = o.o_orderkey
    join {{source('source_millage', 'customer')}} AS c
    ON o.o_custkey = c.c_custkey
    join {{source('source_millage', 'nation')}} AS n
    ON c.c_nationkey = n.n_nationkey
    where o.o_orderdate between '1993-07-01' and '1993-09-30'
   order by 7 desc
   limit 20 
    )
select * from top_20_customers_with_revenue_loss



