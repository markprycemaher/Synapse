/****** Object:  StoredProcedure [DBA_Maintenance].[sp_Populate_Stats_with_deviation]    Script Date: 11/08/2021 16:14:16 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
alter PROC [dbo].[sp_stats_gather] AS

IF OBJECT_ID('dbo.stats_maint_history') IS NOT NULL
  BEGIN;
	
	IF OBJECT_ID('dbo.stats_maint') IS NOT NULL
		BEGIN;
			INSERT INTO dbo.stats_maint_history
				SELECT * FROM dbo.stats_maint ;
		END;
  END;

IF OBJECT_ID('dbo.stats_maint') IS NOT NULL
  BEGIN;
	DROP TABLE dbo.stats_maint ;
  END;

Create table dbo.stats_maint
WITH ( DISTRIBUTION = ROUND_ROBIN) 
AS 
SELECT  getdate() as loaddate,
		cast(''  as varchar(50)) as [Status],
		cast(0 as bigint) as durationms,
		getdate() as last_update ,
		cast(''  as varchar(4000)) as [sqlscript],
		objIdsWithStats.[object_id], 
		actualRowCounts.[schema], 
		actualRowCounts.[logical_table_name], 
		statsRowCounts.stats_row_count, 
		actualRowCounts.actual_row_count,
		row_count_difference = CASE
		WHEN actualRowCounts.actual_row_count >= statsRowCounts.stats_row_count THEN actualRowCounts.actual_row_count - statsRowCounts.stats_row_count
		ELSE statsRowCounts.stats_row_count - actualRowCounts.actual_row_count
	END,
	    percent_deviation_from_actual = CASE
		WHEN actualRowCounts.actual_row_count = 0 THEN statsRowCounts.stats_row_count
		WHEN statsRowCounts.stats_row_count = 0 THEN actualRowCounts.actual_row_count
		WHEN actualRowCounts.actual_row_count >= statsRowCounts.stats_row_count THEN CONVERT(NUMERIC(18, 0), CONVERT(NUMERIC(18, 2), (actualRowCounts.actual_row_count - statsRowCounts.stats_row_count)) / CONVERT(NUMERIC(18, 2), actualRowCounts.actual_row_count) * 100)
		ELSE CONVERT(NUMERIC(18, 0), CONVERT(NUMERIC(18, 2), (statsRowCounts.stats_row_count - actualRowCounts.actual_row_count)) / CONVERT(NUMERIC(18, 2), actualRowCounts.actual_row_count) * 100)
	END
	from
	(
		select distinct object_id from sys.stats -- where stats_id > 1
	) objIdsWithStats
	left join
	(
		select object_id, sum(rows) as stats_row_count from sys.partitions group by object_id
	) statsRowCounts

	on objIdsWithStats.object_id = statsRowCounts.object_id 

	left join
	(
		SELECT sm.name [schema] ,
		tb.name [logical_table_name] ,
		tb.object_id object_id ,
		SUM(rg.row_count) actual_row_count
		FROM sys.schemas sm
		INNER JOIN sys.tables tb ON sm.schema_id = tb.schema_id
		INNER JOIN sys.pdw_table_mappings mp ON tb.object_id = mp.object_id
		INNER JOIN sys.pdw_nodes_tables nt ON nt.name = mp.physical_name
		INNER JOIN sys.dm_pdw_nodes_db_partition_stats rg
		ON rg.object_id = nt.object_id
		AND rg.pdw_node_id = nt.pdw_node_id
		AND rg.distribution_id = nt.distribution_id
		WHERE 1 = 1
		GROUP BY sm.name, tb.name, tb.object_id
	) actualRowCounts
	on objIdsWithStats.object_id = actualRowCounts.object_id 
	
IF OBJECT_ID('dbo.stats_maint_history') IS  NULL
  BEGIN;
	CREATE TABLE dbo.stats_maint_history with (distribution= round_robin) as
		SELECT * FROM dbo.stats_maint where 1=0;
  END;



  -- exec  [dbo].[sp_stats_maint_collect]
 -- select * from dbo.stats_maint;
 -- drop table stats_maint_history;



