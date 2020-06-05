--select * from sys.dm_pdw_exec_requests where status = 'Running'	;

declare @pid as varchar(50)	;
declare @total_bytes  as bigint
declare @done_bytes  as bigint
declare @done_time  as bigint

--@done_bytes,@done_time  as double	  ;


set @pid = 'QID5558'
select * from sys.dm_pdw_dms_external_work			  where request_id =   @pid

declare @starttime datetime;
declare @currenttime datetime;
declare @runningtime bigint;

select @starttime = min(start_time) from sys.dm_pdw_dms_external_work	where request_id =   @pid  
set @currenttime = getdate()

set @runningtime = datediff(mi, @starttime, @currenttime) 

select @runningtime

select count(*),status from sys.dm_pdw_dms_external_work	where request_id =   @pid  	 group by status 

select @total_bytes = sum(length) 	  from sys.dm_pdw_dms_external_work	where request_id =   @pid
select @done_bytes = sum(length),  @done_time  = sum(total_elapsed_time) /1000	  from sys.dm_pdw_dms_external_work	where request_id =   @pid and status = 'Done'
 
 print 	'Total MB: ' + cast((@total_bytes/1024/1024) as varchar(200))
 print	'MB completed: ' + cast((@done_bytes/1024/1024) as varchar(200))
 print 	'Process Time elapsed: ' +  cast( @done_time/60/60 as varchar(200)) + ' minute(s)'
 print 'Wall clock time elapsed: ' + cast(@runningtime as varchar(200))  + ' minute(s)'
 declare @perc_complete as decimal(18,2)
 declare @d_completed_mins as decimal(18,2)
 set @perc_complete = cast(@done_bytes as decimal(18,2))/cast(@total_bytes as decimal(18,2))*100.00

 set @d_completed_mins = cast(@total_bytes  as decimal(18,2))/cast(@done_bytes as decimal(18,2))  *  @runningtime


print 'Percentage complete by size: ' + cast(@perc_complete as varchar(20))


print 'Estimated Time to complete: ' + cast(((cast(@total_bytes  as decimal(18,2))/cast(@done_bytes as decimal(18,2)) ) *  @runningtime)  as varchar(200)) + ' minutes'

print 'Estimated Completion Time: ' + cast(dateadd(mi,  @d_completed_mins ,@starttime) as varchar(200)) 