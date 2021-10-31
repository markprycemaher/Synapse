-- https://docs.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-get-audit-file-transact-sql?view=sql-server-ver15

SELECT * FROM sys.fn_get_audit_file ('https://{storageacc}.blob.core.windows.net/sqldbauditlogs/{sqlservername}/{dbname}/SqlDbAuditing_Audit/2020-02-17',default,default);

SELECT convert(date,event_time) access_date, event_time, session_server_principal_name,  duration_milliseconds FROM sys.fn_get_audit_file ('https://{storageacc}.blob.core.windows.net/sqldbauditlogs/{sqlservername}/{dbname}/SqlDbAuditing_Audit/2020-02-17',default,default);

SELECT convert(date,event_time) access_date, count(*), session_server_principal_name,  sum(duration_milliseconds) FROM sys.fn_get_audit_file ('https://{storageacc}.blob.core.windows.net/sqldbauditlogs/{sqlservername}/{dbname}/SqlDbAuditing_Audit/2020-02-17',default,default)
group by convert(date,event_time), session_server_principal_name;


SELECT convert(date,event_time) access_date, server_instance_name,  session_server_principal_name,  sum(duration_milliseconds) total_duration_milliseconds,count(*) queries FROM sys.fn_get_audit_file ('https://{storageacc}.blob.core.windows.net/sqldbauditlogs/{sqlservername}/{dbname}/SqlDbAuditing_Audit',default,default)
where len(session_server_principal_name) > 0
group by convert(date,event_time), server_instance_name, session_server_principal_name
order by convert(date,event_time);

