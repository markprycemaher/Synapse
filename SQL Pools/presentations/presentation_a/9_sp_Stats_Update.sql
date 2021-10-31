create PROC [dbo].[sp_Stats_Update] AS

-- these figures are for testing and need to be adjusted for the system
declare @percent_smalltable int = 10;
declare @percent_largetable int = 5; 
declare @rows_boundary bigint = 60000000;   --    600,000,000
declare @threshold_for_sample  bigint = 60000000; -- 600,000,000

IF OBJECT_ID('tempdb..#updatestats') IS NOT NULL
  BEGIN;
	DROP TABLE #updatestats;
  END;

CREATE TABLE #updatestats
WITH
(
	DISTRIBUTION   = HASH([seq_nmbr])
)
AS 
SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS [seq_nmbr],
[schema],[logical_table_name],
('update statistics ' + [schema] +'.'+ [logical_table_name] +' WITH FULLSCAN') as query,
('update statistics ' + [schema] +'.'+ [logical_table_name] +' WITH SAMPLE 20 PERCENT') as sample_query,
actual_row_count
FROM dbo.stats_maint where status = ''
AND
-- if the table is VERY LARGE, then 10% of changes is a lot of changes, so I have used 5%.
-- i.e. a 600,000,000 row table, 10% difference is 60,000,000,   30,000,000  is 5%. 
((percent_deviation_from_actual > @percent_smalltable and actual_row_count <= @rows_boundary )
or
(percent_deviation_from_actual > @percent_largetable and actual_row_count > @rows_boundary ))
-- only work on a limited number of schemas
--and  [schema] IN('jeff','tim')
-- and [logical_table_name] not like '%tmp%'  -- do we need to exclude temp tables? They are truncated on load.


declare @duration bigint;
declare @indexStart datetime2;

DECLARE
	@i INT = 1
	, @t INT = (SELECT COUNT(*) FROM #updatestats)
	, @statement NVARCHAR(4000)   = N''
	, @table_name varchar(255)
	, @Schema_name varchar(255)
	, @index_finish_datetime datetime2
	, @sample_query NVARCHAR(4000)   = N''
    , @actual_row_count bigint =0;

WHILE @i <= @t
  BEGIN
	
	SELECT  @statement = query, -- update with fulllscan
			@table_name = [logical_table_name] ,
			@Schema_name = [schema] ,
			@sample_query =sample_query , -- update with sample
			@actual_row_count = actual_row_count
	FROM #updatestats WHERE seq_nmbr = @i;

	set @indexStart = getdate()

	-- if the actual rows is over @threshold_for_sample we switch to sample
	if @actual_row_count >  @threshold_for_sample 
	begin
		set @statement  = @sample_query
	end 

	update dbo.stats_maint
	set [status] = 'Rebuilding...'
		, last_update = @indexStart
		, sqlscript = @statement 
	where @table_name = [logical_table_name]  and @Schema_name = [schema] 


	PRINT @statement
	print @sample_query;

    BEGIN TRY
		
		EXEC sp_executesql @statement

		set @index_finish_datetime = getdate()

		update dbo.stats_maint
			set [status] = 'Updated'
			, last_update = @index_finish_datetime 
			, durationms = datediff("ms", @indexStart, @index_finish_datetime ) 
			where @table_name = [logical_table_name]  and @Schema_name = [schema] 

    END TRY
	BEGIN CATCH
		PRINT 'Opps - something went wrong....'

			set @index_finish_datetime = getdate()

			update dbo.stats_maint
			set [status] = 'Error'
			, last_update = @index_finish_datetime
			, durationms = datediff("ms", @indexStart, @index_finish_datetime) 
			where @table_name = [logical_table_name]  and @Schema_name = [schema] 

	END CATCH
	SET @i+=1;
END

DROP TABLE #updatestats ;
-- exec [dbo].[sp_Update_stats]