/* TPC_H  Query 1 - Pricing Summary Report */

/* 
-- https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/develop-materialized-view-performance-tuning


1) What's a materialized view....
A materialized view pre-computes, stores, and maintains its data
in dedicated SQL pool just like a table. Recomputation isn't needed
each time a materialized view is used. That's why queries that use
all or a subset of the data in materialized views can gain faster
performance. Even better, queries can use a materialized view 
without making direct reference to it, so there's no need to 
change application code.

2) What's 'explain'
3) What's 'explain with_recommendations'
-- https://docs.microsoft.com/en-us/sql/t-sql/queries/explain-transact-sql?view=azure-sqldw-latest&preserve-view=true
4) 
*/
--explain with_recommendations


select * from 
    lineitem;

select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count_big(*) as count_order
from
    lineitem
where
    l_shipdate <= dateadd(day,-90,cast ('1998-12-01' as date))
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus


	/*
	drop view view1

	CREATE MATERIALIZED VIEW View1 WITH (DISTRIBUTION = HASH([Expr0]))
	AS  SELECT [tpch].[dbo].[lineitem].[l_returnflag] AS [Expr0],  
	[tpch].[dbo].[lineitem].[l_linestatus] AS [Expr1],       
	[tpch].[dbo].[lineitem].[l_shipdate] AS [Expr2],      
	SUM([tpch].[dbo].[lineitem].[l_quantity]) AS [Expr3],     
	SUM([tpch].[dbo].[lineitem].[l_extendedprice]) AS [Expr4], 
	SUM([tpch].[dbo].[lineitem].[l_extendedprice]*((1.0000000000000000e+000)-[tpch].[dbo].[lineitem].[l_discount])) AS [Expr5],         SUM([tpch].[dbo].[lineitem].[l_extendedprice]*((1.0000000000000000e+000)-[tpch].[dbo].[lineitem].[l_discount])*((1.0000000000000000e+000)+[tpch].[dbo].[lineitem].[l_tax])) AS [Expr6],         AVG([tpch].[dbo].[lineitem].[l_quantity]) AS [Expr7],         AVG([tpch].[dbo].[lineitem].[l_extendedprice]) AS [Expr8],         AVG([tpch].[dbo].[lineitem].[l_discount]) AS [Expr9],         COUNT_BIG(*) AS [Expr10]  FROM [dbo].[lineitem] 
	GROUP BY [tpch].[dbo].[lineitem].[l_returnflag],    
	[tpch].[dbo].[lineitem].[l_linestatus],      
	[tpch].[dbo].[lineitem].[l_shipdate]
	*/
