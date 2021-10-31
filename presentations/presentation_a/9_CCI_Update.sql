alter PROC [dbo].[sp_CCI_Update] AS


IF OBJECT_ID('tempdb..#updatecci') IS NOT NULL
  BEGIN;
	DROP TABLE #updatecci;
  END;

CREATE TABLE #updatecci
WITH
(
	DISTRIBUTION   = HASH([seq_nmbr])
)
AS 
SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS [seq_nmbr],
 * from dbo.cci_maint 
  where 
  (( abs([COMPRESSED_rowgroup_rows_DELETED]-[row_count_total] ) / convert(decimal,[row_count_total]+1) * 100 ) < 90
  or 
        [COMPRESSED_rowgroup_rows_DELETED] > 10000
  )
 or ( 
 abs([OPEN_rowgroup_rows]) > 100000
or
(		
 abs([OPEN_rowgroup_rows]-[row_count_total] )/ convert(decimal,[row_count_total]+1) * 100 <90
 and [OPEN_rowgroup_rows] > 999
 ) 
 )

declare @duration bigint;
declare @indexStart datetime2;

DECLARE
	@i INT = 1
	, @t INT = (SELECT COUNT(*) FROM #updatecci)
	, @statement NVARCHAR(4000)   = N''
	, @table_name varchar(255)
	, @Schema_name varchar(255)
	, @index_finish_datetime datetime2
	, @sample_query NVARCHAR(4000)   = N''
    , @actual_row_count bigint =0
	, @threshold_for_sample  bigint =0 ;

WHILE @i <= @t
  BEGIN
	
	SELECT  @statement = [Rebuild_Index_SQL], -- update with fulllscan
			@table_name = [table_name] ,
			@Schema_name = [schema_name] ,
			@actual_row_count =[row_count_total] 
	FROM #updatecci WHERE seq_nmbr = @i;

	set @indexStart = getdate()

	-- if the actual rows is over @threshold_for_sample we switch to sample
	if @actual_row_count >  @threshold_for_sample 
	begin
		set @statement  = @sample_query
	end 

	update dbo.cci_maint
	set [status] = 'Rebuilding...'
		, last_update = @indexStart
		, sqlscript = @statement 
	where @table_name = [table_name]  and @Schema_name = [schema_name] 


	PRINT @statement
	print @sample_query;

    BEGIN TRY
		
		EXEC sp_executesql @statement

		set @index_finish_datetime = getdate()

		update dbo.cci_maint
			set [status] = 'Updated'
			, last_update = @index_finish_datetime 
			, durationms = datediff("ms", @indexStart, @index_finish_datetime ) 
	where @table_name = [table_name]  and @Schema_name = [schema_name] 

    END TRY
	BEGIN CATCH
		PRINT 'Opps - something went wrong....'

			set @index_finish_datetime = getdate()

			update dbo.cci_maint
			set [status] = 'Error'
			, last_update = @index_finish_datetime
			, durationms = datediff("ms", @indexStart, @index_finish_datetime) 
			where @table_name = [table_name]  and @Schema_name = [schema_name] 
	END CATCH
	SET @i+=1;
END

DROP TABLE #updatecci ;
-- exec [dbo].[sp_CCI_Update]
-- exec [dbo].[sp_cci_gather]
-- select * from dbo.cci_maint;
/*
ALTER INDEX ALL ON dbo.streaming_customer_test REORGANIZE;;
ALTER INDEX ALL ON dbo.streaming_customer_bulk REORGANIZE;;
ALTER INDEX ALL ON dbo.region REORGANIZE;;
delete from dbo.streaming_customer_test  ;

*/


