/****** Object:  StoredProcedure [microsoft].[sp_update_stats]    Script Date: 29/06/2020 16:01:13 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROC [dbo].[sp_update_stats] 
AS

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
select 'UPDATE STATISTICS ' + two_part_name + ' ' + [stats_name]   as query
	, ROW_NUMBER()
	OVER(ORDER BY (SELECT NULL))    AS [seq_nmbr] , stats_last_updated_date from (
SELECT 
        sm.[name]                                                                AS [schema_name]
,        tb.[name]                                                                AS [table_name]
, co.name																			as [Column_Name]
,        st.[name]                                                                AS [stats_name]
,        st.[has_filter]                                                            AS [stats_is_filtered]
,       ROW_NUMBER()
        OVER(ORDER BY (SELECT NULL))                                            AS [seq_nmbr]
,                                 QUOTENAME(sm.[name])+'.'+QUOTENAME(tb.[name])  AS [two_part_name]
,        QUOTENAME(DB_NAME())+'.'+QUOTENAME(sm.[name])+'.'+QUOTENAME(tb.[name])  AS [three_part_name]
,  STATS_DATE(st.[object_id],st.[stats_id])   AS [stats_last_updated_date]
, st.[user_created] 
FROM    sys.objects            AS ob
JOIN    sys.stats            AS st    ON    ob.[object_id]        = st.[object_id]
JOIN    sys.stats_columns    AS sc    ON    st.[stats_id]        = sc.[stats_id]
                                    AND st.[object_id]        = sc.[object_id]
JOIN    sys.columns            AS co    ON    sc.[column_id]        = co.[column_id]
                                    AND    sc.[object_id]        = co.[object_id]
JOIN    sys.tables            AS tb    ON    co.[object_id]        = tb.[object_id]
JOIN    sys.schemas            AS sm    ON    tb.[schema_id]        = sm.[schema_id]
WHERE    1=1  and STATS_DATE(st.[object_id],st.[stats_id])   is not null
and STATS_DATE(st.[object_id],st.[stats_id]) < getdate() -7
--AND        st.[user_created]   = 1
) a 


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


-- 