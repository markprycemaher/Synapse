/*
Monitor users and resource groups

create proc monitor_logins
as
begin


IF OBJECT_ID('dbo.tmp_user') IS  NULL
  BEGIN;
create table tmp_user with (distribution = round_robin) as
select distinct getdate() dt, s.session_id 
	,s.login_name
	,s.client_id
	, s.app_name
	, r.resource_class
	, r.importance
	, r.group_name
	, r.classifier_name
	, r.resource_allocation_percentage
	from  sys.dm_pdw_exec_requests  r inner join sys.dm_pdw_exec_sessions s 
on r.session_id = s.session_id where 1=0	
  END;

insert into tmp_user
select distinct s.login_time dt, s.session_id 
	,s.login_name
	,s.client_id
	, s.app_name
	, r.resource_class
	, r.importance
	, r.group_name
	, r.classifier_name
	, r.resource_allocation_percentage
	from  sys.dm_pdw_exec_requests  r inner join sys.dm_pdw_exec_sessions s 
on r.session_id = s.session_id
where s.login_time > (select isnull(max(dt),getdate()-5) from tmp_user)

end


exec  monitor_logins

select * from tmp_user;


*/
