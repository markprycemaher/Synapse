create PROC [dbo].[sp_kill_transactions] AS

-- exec [dbo].[sp_kill_transactions]

IF OBJECT_ID('tempdb..#stats_ddl') IS NOT NULL
  BEGIN;
	DROP TABLE #stats_ddl;
  END;

CREATE TABLE #stats_ddl
WITH
(
	DISTRIBUTION   = ROUND_ROBIN
)
AS 
SELECT distinct 'kill ''' + waits.session_id + '''' as sessionid, ROW_NUMBER()
			OVER(ORDER BY (SELECT NULL))    AS [seq_nmbr] 
  /*,   waits.request_id,  
      requests.command,
      requests.status,
      requests.start_time,  
      waits.type,
      waits.state,
      waits.object_type,
      waits.object_name*/
FROM   sys.dm_pdw_waits waits
   JOIN  sys.dm_pdw_exec_requests requests
   ON waits.request_id=requests.request_id
WHERE waits.[type] = 'ExclusiveUpdate'
and datediff(s,requests.start_time,getdate()) > 600 -- time in seconds
--ORDER BY waits.object_name, waits.object_type, waits.state;


DECLARE
	@i INT = 1
	, @t INT = (SELECT COUNT(*) FROM #stats_ddl)
	, @sessionid NVARCHAR(50)   = N'';

WHILE @i <= @t
  BEGIN
	SET  @sessionid = (SELECT sessionid FROM #stats_ddl WHERE seq_nmbr = @i);

	PRINT @sessionid
    BEGIN TRY
		EXEC sp_executesql @sessionid
    END TRY
	BEGIN CATCH
		PRINT 'Opps - something went wrong....'
	END CATCH
	SET @i+=1;
END

DROP TABLE #stats_ddl;