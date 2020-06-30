/****** Object:  StoredProcedure [microsoft].[sp_rebuild_index]    Script Date: 29/06/2020 17:11:15 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [dbo].[sp_rebuild_index] AS


IF OBJECT_ID('tempdb..#stats_ddl') IS NOT NULL
  BEGIN;
	DROP TABLE #stats_ddl;
  END;

CREATE TABLE #stats_ddl
WITH
(
	DISTRIBUTION   = HASH([seq_nmbr])
	, LOCATION     = USER_DB
)
AS 
select 'ALTER INDEX ALL ON  ' + s.name + '.' + t.name + ' REBUILD ' as query
	, ROW_NUMBER()
			OVER(ORDER BY (SELECT NULL))    AS [seq_nmbr] 
			 from sys.tables t inner join sys.schemas s on s.schema_id = t.schema_id  


DECLARE
	@i INT = 1
	, @t INT = (SELECT COUNT(*) FROM #stats_ddl)
	, @statement NVARCHAR(4000)   = N'';

WHILE @i <= @t
  BEGIN
	SET @statement = (SELECT query FROM #stats_ddl WHERE seq_nmbr = @i);

	PRINT @statement
    BEGIN TRY
		EXEC sp_executesql @statement
    END TRY
	BEGIN CATCH
		PRINT 'Opps - something went wrong....'
	END CATCH
	SET @i+=1;
END

DROP TABLE #stats_ddl;
