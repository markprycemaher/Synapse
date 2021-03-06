/****** Object:  StoredProcedure [dbo].[Partition_load_Stage1]    Script Date: 13/03/2020 14:19:13 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROC [dbo].[Partition_load_Stage1] @hDAY [varchar](12),@SourceSchema [varchar](50),@SourceTbl [varchar](50),@DestSchema [varchar](50),@DestTbl [varchar](50) AS
begin
	declare @sSQLSplitSource varchar(4000);
	declare @sSQLSplitDestination varchar(4000);
	declare @sSQLSwap varchar(4000);
	declare @sSQLStats varchar(4000);
	-- load data into the stage table

	declare @username [varchar](50) ='loaderuser'
	declare @resourceclass [varchar](50) = 'xlargerc'
	declare @dwuc [varchar](50) = '400'

	declare @procname varchar(50) = 'Partition_load_Stage1'

	exec logit @procname,'Start', @username, @resourceclass,  @dwuc

	declare @SourceTablename varchar(50) = '[' + @SourceSchema + '].[' + @SourceTbl + ']'
	declare @DestinationTablename varchar(50) = '[' + @DestSchema + '].[' + @DestTbl + ']'


	--Dynamicially create paritions based upton the data we are loading each day
	set @sSQLSplitSource = 'ALTER TABLE ' + @SourceTablename + ' SPLIT RANGE (''' + @hDAY + ''');'
	exec (@sSQLSplitSource)
	print @sSQLSplitSource


	set @sSQLSplitDestination = 'ALTER TABLE ' + @DestinationTablename + ' SPLIT RANGE (''' + @hDAY + ''');'
	exec (@sSQLSplitDestination)
	print @sSQLSplitDestination

	exec logit @procname,'End', @username, @resourceclass,  @dwuc

	END

