-- top 10 slowest queries
select top 10 t.total_elapsed_time, * from sys.dm_pdw_exec_requests t order by t.total_elapsed_time desc;

-- top 10 waintg.
select total_elapsed_time, * from (
select  datediff(mi, submit_time,start_time)  t1
		, datediff(mi, submit_time,end_compile_time)  t2
		, datediff(mi, start_time,end_compile_time)  t3
		, *  from sys.dm_pdw_exec_requests t
		) t
	--	where t1 != 0 or t2 != 0 or t3 != 0
order by t3 desc;



