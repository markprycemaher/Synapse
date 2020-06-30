CREATE VIEW [dbo].[vw_dm_pdw_exec_requests2]
AS select r.*
,s.status exec_sessions_status
,s.security_id
,s.login_name
,s.login_time
,s.query_count
,s.is_transactional
,s.client_id
,s.app_name
,s.sql_spid
from [dbo].[vw_dm_pdw_exec_requests] r 
left join sys.dm_pdw_exec_sessions s on r.session_id = s.session_id