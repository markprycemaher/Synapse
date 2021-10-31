/* https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/performance-tuning-result-set-caching */
/* TPC_H  Query 1 - Pricing Summary Report */
/*
Excute 3 times, RSC off, RSC on cold, RSC hot
*/-- takes between 12 seconds

-- result set caching
/*
ALTER DATABASE [tpch] SET RESULT_SET_CACHING off;
ALTER DATABASE [tpch] SET RESULT_SET_CACHING ON; -- use master

select * from sys.dm_pdw_exec_requests where [label]='rsc test 1' order by submit_time desc;

SELECT name, is_result_set_caching_on FROM sys.databases;

SELECT * FROM sys.dm_pdw_request_steps
WHERE request_id = 'QID6157607'
ORDER BY step_index;

/*
QID6157607
QID6157602
QID6157579 */

-- RSC survives restarts... and size changes

SELECT sum(total_elapsed_time) FROM sys.dm_pdw_request_steps
WHERE request_id = 'QID32170'

*/


--explain with_recommendations
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
	option(label='rsc test 2')


/*


-- RSC off - 5 steps.
-- RSC on - 9 steps- (creates Qtable) (cold)
-- RSC on - 1 step


*/

