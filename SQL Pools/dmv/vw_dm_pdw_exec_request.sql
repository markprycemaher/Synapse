
CREATE VIEW [dbo].[vw_dm_pdw_exec_requests]
AS select 
	datediff(ms,submit_time,start_time) as wait_time ,  
	datediff(ms,start_time, end_compile_time) as compile_time ,
	datediff(ms,end_compile_time, end_time) as query_time , 
	datediff(ms,start_time, end_time) as execution_time , 
	*,
	case isnull(result_cache_hit,99)
		when 1 then 'Result set cache hit'
		when 0 then 'Result set cache miss'
		when -1 then 'Result set caching is disabled on the database.'
		when -2 then 'Result set caching is disabled on the session.'
		when -4 then 'Result set caching is disabled due to no data sources for the query.'
		when -8 then 'Result set caching is disabled due to row level security predicates.'
		when -16 then 'Result set caching is disabled due to the use of system table, temporary table, or external table in the query.'
		when -32 then 'Result set caching is disabled because the query contains runtime constants, user-defined functions, or non-deterministic functions.'
		when -64 then 'Result set caching is disabled due to estimated result set size is >10GB.'
		when -128 then 'Result set caching is disabled because the result set contains rows with large size (>64kb).'
		when 99 then 'Null'
		else  'Unknown' end result_cache_hit_desc

	from sys.dm_pdw_exec_requests;
GO


