DECLARE @run_type varchar(10) ='FULL' /*accepted values: FULL,SCHEMA,TABLE*/
DECLARE @report_type [varchar](10) = 'INSERT' /*accepted values: CTAS,INSERT*/
DECLARE @stage_cleanse_type [varchar](10) = 'DROP'/*accepted values: DROP,KEEP*/
DECLARE @op_schema_name varchar(100) = 'dbo'

EXEC dbo.[sp_health_check_report]  @run_type, @report_type, @stage_cleanse_type, @op_schema_name, NULL, NULL

select * from [dbo].[HC_REPORT]

/*
CREATE PROC dbo.[sp_health_check_report] 
            @run_type [varchar](10) /*accepted values: FULL,SCHEMA,TABLE*/
           ,@report_type [varchar](10) /*accepted values: CTAS,INSERT*/
           ,@stage_cleanse_type [varchar](10) /*accepted values: DROP,KEEP*/
           ,@op_schema_name [varchar](100) /*schema name that will contain created tables for the report calculation*/
           ,@hc_schema_name [varchar](100) /*schema name that will be scanned for report, used only for SCHEMA and TABLE run type*/
		   ,@hc_table_name [varchar](300)  /*table name that will be scanned for report, used only for  TABLE run type*/

AS
/****************************************************************************************************************************** 
    About:  This Stored Procedure uses metrics calculated from Azure Synapse SQL Pools DMVs to build a health check report base table.
    After creating the base table, the proc calculates scores and flags and from them an importance score value to identify most problematic tables.
            DMVs used in this SP to create the HealthCheck Report are:
            SYS.OBJECTS
            SYS.SCHEMAS
            SYS.TABLES 
            SYS.ALL_COLUMNS
            SYS.COLUMNS
            SYS.STATS
            SYS.TYPES
            SYS.PDW_TABLE_MAPPINGS
            SYS.PDW_DISTRIBUTIONS
            SYS.DM_PDW_NODES
            SYS.PDW_NODES_TABLES
            SYS.DM_PDW_NODES_DB_COLUMN_STORE_ROW_GROUP_PHYSICAL_STATS
            SYS.DM_PDW_NODES_DB_PARTITION_STATS
            SYS.PDW_TABLE_DISTRIBUTION_PROPERTIES
            SYS.PDW_COLUMN_DISTRIBUTION_PROPERTIES
            SYS.PARTITIONS
            SYS.INDEXES
 the maintenance for this SP will be shared via below github repository, unless another information is shared 
 again in below repository.
   https://github.com/sasever/SQLPoolsTools     
 
 Syntax rules:
            Syntax rule 1: SP inputs are named with snakecase (ex:input_variable_name)
            Syntax rule 2: SP local variables are named with camelcase (ex:localVariableName)
SP does not handle the cleanup/housekeeping of the report artifacts.
you can add a block to clean up all artifacts except the HC_REPORT tables to the procedure, or add an additional clean up methodology.
******************************************************************************************************************************/
BEGIN

    SET NOCOUNT ON

    IF @run_type not in ('FULL','SCHEMA','TABLE') or @report_type not in ('CTAS','INSERT') or @stage_cleanse_type not in ('DROP','KEEP')
    BEGIN
        DECLARE @returnMessage nVARCHAR(400) = 'One or more values for given input parameter config @run_type:['+ISNULL(@run_type,'NULL')+'] & @report_type:['+ISNULL(@report_type,'NULL')+'] & @stage_cleanse_type:['+ISNULL(@stage_cleanse_type,'NULL')+'] is undefined!'
        RAISERROR (@returnMessage,-1,-1,'sp_healthcheck'  );
    END
    ELSE
    BEGIN
        DECLARE @runDateSuffix nVARCHAR(10) = FORMAT(getdate() ,'yyyyMMdd') 
        DECLARE @tableRunTag nVARCHAR(100) =  '_'+@run_type+'_'+@runDateSuffix
        DECLARE @queryText nVARCHAR(4000)
        DECLARE @queryText2 nVARCHAR(4000)
        DECLARE @queryText3 nVARCHAR(4000)
        DECLARE @queryText4 nVARCHAR(4000)
        DECLARE @queryText5 nVARCHAR(4000)
        DECLARE @queryText6 nVARCHAR(4000)

        DECLARE @queryTargetTable nVARCHAR(200)

        SET  @queryTargetTable = @op_schema_name+'.HC_TABLE_PATTERN'+@tableRunTag
        SET  @queryText =
            N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                DROP  TABLE '+@queryTargetTable+'
            CREATE TABLE '+@queryTargetTable+'
            with(distribution=hash(object_id),
                heap)
            as
            with mschema as(
            select distinct t.object_id,s.name schemaname,t.name as tablename
                ,c.name as columnname
                ,c.column_id as column_id
                ,ty.name as typename
                ,c.max_length  
                ,c.precision
                ,c.scale
                ,c.is_nullable
                ,c.collation_name
            from sys.schemas s 
            join sys.tables t       on s.schema_id=t.schema_id
            join sys.all_columns c  on t.object_id=c.object_id
            join [sys].[types] ty   on ty.system_type_id=c.system_type_id '
            +case when @run_type='TABLE' then 
            '               and s.name='''+@hc_schema_name+''' and t.name='''+@hc_table_name+''' '
                when @run_type='SCHEMA' then 
            '               and s.name='''+@hc_schema_name+''' '
                else ''
            end
            +' and ty.name  != ''sysname'')
            select   
            object_id
            ,tablename
            ,schemaname
            ,sum(case when typename in(''varchar'',''nvarchar'') and max_length>32 then 1 else 0 end) as num_of_big_string_columns 
            ,avg(case when typename in(''varchar'',''nvarchar'') and max_length>32 then max_length end) as avg_length_of_big_string_columns 
            ,sum(case when typename in(''bigint'',''datetime2'',''float'') then 1 else 0 end) as num_of_other_big_columns 
            ,sum(case when typename like ''%date%'' then 1 else 0 end) as num_of_date_columns 
            ,count(*) num_of_total_columns
            -- string_agg(upper(columnname),'', '') WITHIN GROUP ( ORDER BY column_id ASC)  
            from mschema
            where 1=1
            group by object_id,tablename,schemaname;
			'

        --/**/PRINT(@queryText)  
        EXEC (@queryText) 
        --/**/PRINT(char(10)+'*********************************************************************************'+char(10))
        SET  @queryTargetTable = @op_schema_name+'.HC_COLUMN_STORE_DENSITY'+@tableRunTag
        SET  @queryText = N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                DROP  TABLE '+@queryTargetTable+'
            CREATE TABLE '+@queryTargetTable+'
            with(distribution=hash(object_id),
                heap)
            AS
            SELECT
                    t.object_id                                                             AS object_id
            ,       s.name                                                                  AS [schemaname]
            ,       t.name                                                                  AS [tablename]
            ,       COUNT(DISTINCT rg.[partition_number])                    AS [table_partition_count]
            ,       SUM(rg.[total_rows])                                                    AS [row_count_total]
            ,       SUM(rg.[total_rows])/COUNT(DISTINCT rg.[distribution_id])               AS [row_count_per_distribution_MAX]
            ,    CEILING    ((SUM(rg.[total_rows])*1.0/COUNT(DISTINCT rg.[distribution_id]))/1048576) AS [rowgroup_per_distribution_MAX_IDEAL]
            ,    CEILING    (SUM(rg.[total_rows])*1.0/1048576) AS [rowgroup_total_MAX_IDEAL]
            ,       SUM(CASE WHEN rg.[State] = 0 THEN 1                   ELSE 0    END)    AS [INVISIBLE_rowgroup_count]
            ,       SUM(CASE WHEN rg.[State] = 0 THEN rg.[total_rows]     ELSE 0    END)    AS [INVISIBLE_rowgroup_rows]
            ,       MIN(CASE WHEN rg.[State] = 0 THEN rg.[total_rows]     ELSE NULL END)    AS [INVISIBLE_rowgroup_rows_MIN]
            ,       MAX(CASE WHEN rg.[State] = 0 THEN rg.[total_rows]     ELSE NULL END)    AS [INVISIBLE_rowgroup_rows_MAX]
            ,       AVG(CASE WHEN rg.[State] = 0 THEN rg.[total_rows]     ELSE NULL END)    AS [INVISIBLE_rowgroup_rows_AVG]
            ,       SUM(CASE WHEN rg.[State] = 1 THEN 1                   ELSE 0    END)    AS [OPEN_rowgroup_count]
            ,       SUM(CASE WHEN rg.[State] = 1 THEN rg.[total_rows]     ELSE 0    END)    AS [OPEN_rowgroup_rows]
            ,       MIN(CASE WHEN rg.[State] = 1 THEN rg.[total_rows]     ELSE NULL END)    AS [OPEN_rowgroup_rows_MIN]
            ,       MAX(CASE WHEN rg.[State] = 1 THEN rg.[total_rows]     ELSE NULL END)    AS [OPEN_rowgroup_rows_MAX]
            ,       AVG(CASE WHEN rg.[State] = 1 THEN rg.[total_rows]     ELSE NULL END)    AS [OPEN_rowgroup_rows_AVG]
            ,       SUM(CASE WHEN rg.[State] = 2 THEN 1                   ELSE 0    END)    AS [CLOSED_rowgroup_count]
            ,       SUM(CASE WHEN rg.[State] = 2 THEN rg.[total_rows]     ELSE 0    END)    AS [CLOSED_rowgroup_rows]
            ,       MIN(CASE WHEN rg.[State] = 2 THEN rg.[total_rows]     ELSE NULL END)    AS [CLOSED_rowgroup_rows_MIN]
            ,       MAX(CASE WHEN rg.[State] = 2 THEN rg.[total_rows]     ELSE NULL END)    AS [CLOSED_rowgroup_rows_MAX]
            ,       AVG(CASE WHEN rg.[State] = 2 THEN rg.[total_rows]     ELSE NULL END)    AS [CLOSED_rowgroup_rows_AVG]
            ,       SUM(CASE WHEN rg.[State] = 3 THEN 1                   ELSE 0    END)    AS [COMPRESSED_rowgroup_count]
            ,       SUM(CASE WHEN rg.[State] = 3 THEN rg.[total_rows]     ELSE 0    END)    AS [COMPRESSED_rowgroup_rows]
            ,       SUM(CASE WHEN rg.[State] = 3 THEN rg.[deleted_rows]   ELSE 0    END)    AS [COMPRESSED_rowgroup_rows_DELETED]
            ,       MIN(CASE WHEN rg.[State] = 3 THEN rg.[total_rows]     ELSE NULL END)    AS [COMPRESSED_rowgroup_rows_MIN]
            ,       MAX(CASE WHEN rg.[State] = 3 THEN rg.[total_rows]     ELSE NULL END)    AS [COMPRESSED_rowgroup_rows_MAX]
            ,       AVG(CASE WHEN rg.[State] = 3 THEN rg.[total_rows]     ELSE NULL END)    AS [COMPRESSED_rowgroup_rows_AVG]
            FROM    sys.[pdw_nodes_column_store_row_groups] rg '+char(10)
        SET  @queryText2 = N'            JOIN    sys.[pdw_nodes_tables] nt     ON  rg.[object_id]       = nt.[object_id]
                                                  AND rg.[pdw_node_id]     = nt.[pdw_node_id]
                                                  AND rg.[distribution_id] = nt.[distribution_id]
            JOIN    sys.[pdw_table_mappings] mp   ON  nt.[name]            = mp.[physical_name]
            JOIN    sys.[tables] t                ON  mp.[object_id]       = t.[object_id]
            JOIN    sys.[schemas] s               ON t.[schema_id]         = s.[schema_id] 
			WHERE 1=1 '
            +case when @run_type='TABLE' then 
            '               and s.name='''+@hc_schema_name+''' and t.name='''+@hc_table_name+''' '
                when @run_type='SCHEMA' then 
            '               and s.name='''+@hc_schema_name+''' '
                else ''
            end
            +' GROUP BY t.object_id 
            ,       s.[name]
            ,       t.[name];
			'

        --/**/PRINT(@queryText)  
		--/**/PRINT(@queryText2)
        EXEC (@queryText + @queryText2 ) 
        --/**/PRINT(char(10)+'*********************************************************************************'+char(10))
        
        SET  @queryTargetTable = @op_schema_name+'.HC_GENERAL_ROWGROUP_HEALTH'+@tableRunTag
        SET  @queryText =
            N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                DROP  TABLE '+@queryTargetTable+'
            CREATE TABLE '+@queryTargetTable+'
            with(distribution=hash(object_id),
                heap)
            AS
            with cte as(
                select
                     tb.object_id
                    ,sm.name as schemaname
                    ,tb.[name] AS tablename
                    , rg.[row_group_id] AS [row_group_id]
                    , rg.[state] AS [state] 
                    , rg.[state_desc] AS [state_desc]
                    --, format(rg.[total_rows],''#,#'') AS [total_rows]
                    ,rg.[total_rows]
                    , rg.[trim_reason_desc] AS trim_reason_desc
                    , mp.[physical_name] AS physical_name
                FROM sys.[schemas] sm
                JOIN sys.[tables] tb ON sm.[schema_id] = tb.[schema_id]
                JOIN sys.[pdw_table_mappings] mp ON tb.[object_id] = mp.[object_id]
                JOIN sys.[pdw_nodes_tables] nt ON nt.[name] = mp.[physical_name]
                JOIN sys.[dm_pdw_nodes_db_column_store_row_group_physical_stats] rg ON rg.[object_id] = nt.[object_id]
                AND rg.[pdw_node_id] = nt.[pdw_node_id]
                AND rg.[distribution_id] = nt.[distribution_id] '
            +case when @run_type='TABLE' then 
            '               and sm.name='''+@hc_schema_name+''' and tb.name='''+@hc_table_name+''' '
                when @run_type='SCHEMA' then 
            '               and sm.name='''+@hc_schema_name+''' '
                else ''
            end
            +'
            )
            select object_id,
            schemaname,
            tablename,
            sum( case when trim_reason_desc=''DICTIONARY_SIZE'' then 1 else 0 end)DICTIONARY_SIZE_Trimmed_RG,
            min( case when trim_reason_desc=''DICTIONARY_SIZE'' then [total_rows] end)DICTIONARY_SIZE_Trimmed_RG_max_size,
            min( case when trim_reason_desc=''DICTIONARY_SIZE'' then [total_rows] end)DICTIONARY_SIZE_Trimmed_RG_min_size,
            min( case when trim_reason_desc=''DICTIONARY_SIZE'' then [total_rows] end)DICTIONARY_SIZE_Trimmed_RG_avg_size,
            sum( case when trim_reason_desc=''BULKLOAD'' then 1 else 0 end)BULKLOAD_Trimmed_RG,
            max( case when trim_reason_desc=''BULKLOAD'' then [total_rows] end)BULKLOAD_Trimmed_RG_max_size,
            min( case when trim_reason_desc=''BULKLOAD'' then [total_rows] end)BULKLOAD_Trimmed_RG_min_size,
            avg( case when trim_reason_desc=''BULKLOAD'' then [total_rows] end)BULKLOAD_Trimmed_RG_avg_size,
            sum( case when trim_reason_desc=''MEMORY_LIMITATION'' then 1 else 0 end)MEMORY_LIMITATION_Trimmed_RG,
            max( case when trim_reason_desc=''MEMORY_LIMITATION'' then [total_rows] end)MEMORY_LIMITATION_Trimmed_RG_max_size,
            min( case when trim_reason_desc=''MEMORY_LIMITATION'' then [total_rows] end)MEMORY_LIMITATION_Trimmed_RG_min_size,
            avg( case when trim_reason_desc=''MEMORY_LIMITATION'' then [total_rows] end)MEMORY_LIMITATION_Trimmed_RG_avg_size
            from cte  group by object_id,tablename,schemaname'

        --/**/PRINT(@queryText)  
        EXEC (@queryText) 
        --/**/PRINT(char(10)+'*********************************************************************************'+char(10))
        
        SET  @queryTargetTable = @op_schema_name+'.HC_DISTRIBUTION_LAYOUT'+@tableRunTag
        SET  @queryText =
            N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                DROP  TABLE '+@queryTargetTable+'
            CREATE TABLE '+@queryTargetTable+'
            with(distribution=hash(object_id),
                heap)
            AS
            SELECT
                distinct t.object_id,
                s.name as SchemaName,
                t.name as TableName,
                Is_external,
                tdp.distribution_policy_desc,
                CASE WHEN cdp.distribution_ordinal = 1 then c.name ELSE NULL END as DistCol,
                Case when cdp.distribution_ordinal = 1 then ty.Name ELSE NULL END as DistCOL_DataType,
                i.type_desc as StorageType,
                CASE WHEN count(p.rows) > 1 THEN ''YES'' ELSE ''NO'' END as IsPartitioned,
                count(p.rows) as NumPartitions,
                sum(p.rows) as NumRows
            FROM
                sys.tables AS t
                INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
                INNER JOIN sys.pdw_table_distribution_properties AS tdp ON t.object_id = tdp.object_id
                INNER JOIN sys.columns AS c ON t.object_id = c.object_id
                INNER join sys.types ty ON C.system_type_id = ty.system_type_id
                INNER JOIN sys.pdw_column_distribution_properties AS cdp ON c.object_id = cdp.object_id
                AND c.column_id = cdp.column_id --note rowcount from sys.partitions assumes PDW stats are accurate
            --to get stats from nodes use pdw_nodes_partitions instead
                INNER JOIN sys.partitions p ON t.object_id = p.object_id
                INNER JOIN sys.indexes i ON t.object_id = i.object_id
            WHERE 1=1 '
            +case when @run_type='TABLE' then 
            '           and s.name='''+@hc_schema_name+''' and t.name='''+@hc_table_name+''' '
                when @run_type='SCHEMA' then 
            '           and s.name='''+@hc_schema_name+''' '
                else ''
            end
            +'and (
                tdp.distribution_policy_desc <> ''HASH''
                or cdp.distribution_ordinal = 1
            )
            AND i.index_ID < 2
            and is_external = ''0''
            GROUP BY
            t.object_id,
            s.name,
            t.name,
            tdp.distribution_policy_desc,
            cdp.distribution_ordinal,
            c.name,
            i.type_desc,
            Is_external,
            ty.Name
            union
            SELECT  t.object_id,
            s.name as SchemaName,
            t.name as TableName,
            Is_external,
            '''' as distribution_policy_desc,
            NULL AS DataType,
            NULL as DistCol,
            ''HADOOP'' StorageType,
            ''NO'' as IsPartitioned,
            0 as NumPartitions,
            0 as NumRows
            FROM   sys.tables AS t
            INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
            Where  Is_External = ''1'' '
            +case when @run_type='TABLE' then 
            '               and s.name='''+@hc_schema_name+''' and t.name='''+@hc_table_name+''' '
                when @run_type='SCHEMA' then 
            '               and s.name='''+@hc_schema_name+''' '
                else ''
            end
            
  
        --/**/PRINT(@queryText)  
        EXEC (@queryText) 
        --/**/PRINT(char(10)+'*********************************************************************************'+char(10))
        
        SET  @queryTargetTable = @op_schema_name+'.HC_DISTRIBUTION_SKEW_INFO'+@tableRunTag
        SET  @queryText =
            N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                DROP  TABLE '+@queryTargetTable+'
            CREATE TABLE '+@queryTargetTable+'
            with(distribution=hash(object_id),
                heap)
            AS
            WITH base
            AS
            (
            SELECT 
            s.name                                                               AS  [schemaname]
            , t.name                                                               AS  [tablename]
            , QUOTENAME(s.name)+''.''+QUOTENAME(t.name)                              AS  [two_part_name]
            , nt.[name]                                                            AS  [node_table_name]
            , ROW_NUMBER() OVER(PARTITION BY nt.[name] ORDER BY (SELECT NULL))     AS  [node_table_name_seq]
            , tp.[distribution_policy_desc]                                        AS  [distribution_policy_name]
            , c.[name]                                                             AS  [distribution_column]
            , nt.[distribution_id]                                                 AS  [distribution_id]
            , i.[index_id]                                                         AS  [index_id]
            , i.[type]                                                             AS  [index_type]
            , i.[type_desc]                                                        AS  [index_type_desc]
            , i.[name]		                                                       AS  [index_name]
            , nt.[pdw_node_id]                                                     AS  [pdw_node_id]
            , pn.[type]                                                            AS  [pdw_node_type]
            , pn.[name]                                                            AS  [pdw_node_name]
            , di.name                                                              AS  [dist_name]
            , di.position                                                          AS  [dist_position]
            , nps.[partition_number]                                               AS  [partition_nmbr]
            , nps.[reserved_page_count]                                            AS  [reserved_space_page_count]
            , nps.[reserved_page_count] - nps.[used_page_count]                    AS  [unused_space_page_count]
            , nps.[in_row_data_page_count] 
                + nps.[row_overflow_used_page_count] 
                + nps.[lob_used_page_count]                                        AS  [data_space_page_count]
            , nps.[reserved_page_count] 
            - (nps.[reserved_page_count] - nps.[used_page_count]) 
            - ([in_row_data_page_count] 
                    + [row_overflow_used_page_count]+[lob_used_page_count])       AS  [index_space_page_count]
            , nps.[row_count]                                                      AS  [approx_row_count]
            , t.[object_id]                                                        AS  [object_id]
            from 
                sys.schemas s
            INNER JOIN sys.tables t
                ON s.[schema_id] = t.[schema_id]
            INNER JOIN sys.indexes i
                ON  t.[object_id] = i.[object_id]
                --AND i.[index_id] <= 1					-- <= 1 will Report only on primary table storage (i.e. do not include in report any NCI)
            INNER JOIN sys.pdw_table_distribution_properties tp
                ON t.[object_id] = tp.[object_id]
            INNER JOIN sys.pdw_table_mappings tm
                ON t.[object_id] = tm.[object_id]
            INNER JOIN sys.pdw_nodes_tables nt
                ON tm.[physical_name] = nt.[name]
            INNER JOIN sys.dm_pdw_nodes pn
                ON  nt.[pdw_node_id] = pn.[pdw_node_id] '+char(10)

             SET  @queryText2 =   N'                AND pn.[type] = ''COMPUTE'' -- need to filter out all but the COMPUTE nodes for data table information 
            -- this was causing doubling of size values reported by users when running on single node (< DWU1000) databases
            INNER JOIN sys.pdw_distributions di
                ON  nt.[distribution_id] = di.[distribution_id]
            INNER JOIN sys.dm_pdw_nodes_db_partition_stats nps
                ON nt.[object_id] = nps.[object_id]
                AND nt.[pdw_node_id] = nps.[pdw_node_id]
                AND i.[index_id] = nps.[index_id]				-- Need to also join on the index id.   
                AND nt.[distribution_id] = nps.[distribution_id]
            LEFT OUTER JOIN (select * from sys.pdw_column_distribution_properties where distribution_ordinal = 1) cdp
                ON t.[object_id] = cdp.[object_id]
            LEFT OUTER JOIN sys.columns c
                ON cdp.[object_id] = c.[object_id]
                AND cdp.[column_id] = c.[column_id] 
            WHERE 1=1 '
            +case when @run_type='TABLE' then 
            '               and s.name='''+@hc_schema_name+''' and t.name='''+@hc_table_name+''' '
                when @run_type='SCHEMA' then 
            '               and s.name='''+@hc_schema_name+''' '
                else ''
            end
            +' )
            , size
            AS
            (
            SELECT
            object_id
            ,  [schemaname]
            ,  [tablename]
            ,  [two_part_name]
            ,  [node_table_name]
            ,  [node_table_name_seq]
            ,  [distribution_policy_name]
            ,  [distribution_column]
            ,  [distribution_id]
            ,  [index_id]
            ,  [index_type]
            ,  [index_type_desc]
            ,  [index_name]
            ,  [pdw_node_id]
            ,  [pdw_node_type]
            ,  [pdw_node_name]
            ,  [dist_name]
            ,  [dist_position]
            ,  [reserved_space_page_count]
            ,  [unused_space_page_count]
            ,  [data_space_page_count]
            ,  [index_space_page_count]
            ,  [approx_row_count]
            ,PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY [approx_row_count])  OVER (PARTITION BY object_id) AS DISTR_median_row_count
            FROM base
            )
            select s.object_id,s.schemaname, s.	tablename, 
                DISTR_max_row_count,DISTR_min_row_count,DISTR_avg_row_count,DISTR_median_row_count,
                case when isnull(DISTR_max_row_count,0)>0 then (DISTR_max_row_count * 1.00 - DISTR_min_row_count * 1.00) /DISTR_max_row_count * 1.00  else -1 end AS DISTR_max_min_skew,
                case when isnull(DISTR_max_row_count,0)>0 then(DISTR_max_row_count * 1.00 - DISTR_avg_row_count * 1.00) / DISTR_max_row_count * 1.00  else -1 end AS DISTR_max_avg_skew,
                case when isnull(DISTR_max_row_count,0)>0 then(DISTR_max_row_count * 1.00 - DISTR_median_row_count * 1.00) / DISTR_max_row_count * 1.00  else -1 end AS DISTR_max_med_skew
            from (
            SELECT object_id,schemaname, 
                tablename, 
                DISTR_median_row_count, 
                MAX([approx_row_count]) as DISTR_max_row_count,
                MIN([approx_row_count]) as DISTR_min_row_count,
                AVG([approx_row_count]) as DISTR_avg_row_count	
            FROM size
            GROUP BY object_id,schemaname, 
                    tablename ,DISTR_median_row_count) as s'

  
        --/**/PRINT(@queryText)  
		--/**/PRINT(@queryText2)
        EXEC (@queryText + @queryText2) 
        --/**/PRINT(char(10)+'*********************************************************************************'+char(10))
        
        SET  @queryTargetTable = @op_schema_name+'.HC_TABLE_STATS_INFO'+@tableRunTag
        SET  @queryText =
            N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                DROP  TABLE '+@queryTargetTable+'
            CREATE TABLE '+@queryTargetTable+'
            with(distribution=hash(object_id),
                heap)
            AS
            WITH STATS
            AS
            (
            SELECT TB.OBJECT_ID,
                sm.[name] AS [schemaname], 
                tb.[name] AS [tablename], 
                st.[name] AS [stats_name], 
                st.[has_filter] AS [stats_is_filtered], 
                STATS_DATE(st.[object_id], st.[stats_id]) AS [stats_last_updated_date], 
                st.[user_created] 
            FROM sys.objects AS ob
                JOIN sys.stats AS st ON ob.[object_id] = st.[object_id] 
                JOIN sys.tables AS tb ON st.[object_id] = tb.[object_id]
                JOIN sys.schemas AS sm ON tb.[schema_id] = sm.[schema_id]
            WHERE 1 = 1 
             AND STATS_DATE(st.[object_id], st.[stats_id]) IS NOT NULL '
            +case when @run_type='TABLE' then 
            '               and sm.name='''+@hc_schema_name+''' and tb.name='''+@hc_table_name+''' '
                when @run_type='SCHEMA' then 
            '               and sm.name='''+@hc_schema_name+''' '
                else ''
            end
            +' )
            select OBJECT_ID,[schemaname],[tablename]
                ,max([stats_last_updated_date]) newest_active_stat_date
                ,min([stats_last_updated_date]) oldest_active_stat_date
                ,count(*) number_of_stats 
                ,SUM(cast([user_created] as int)) NUM_OF_USERCREATED_STATS
                ,SUM(cast([stats_is_filtered] as int)) NUM_OF_FILTERED_STATS
            from STATS  
            group by  OBJECT_ID,[schemaname],[tablename]'
  
        --/**/PRINT(@queryText)  
        EXEC (@queryText) 
        --/**/PRINT(char(10)+'*********************************************************************************'+char(10))
        
        SET  @queryTargetTable = @op_schema_name+'.HC_BASE'+@tableRunTag
        SET  @queryText =
            N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                DROP  TABLE '+@queryTargetTable+'
            CREATE TABLE '+@queryTargetTable+'
            WITH 
                (
                    DISTRIBUTION = hash(TableName),
                    HEAP
                )
                    AS
        SELECT 
            getdate() AS HC_DATE, isnull( DL.NUMROWS,CSD.ROW_COUNT_TOTAL) as TOTAL_ROW_COUNT,  TP.OBJECT_ID, TP.TABLENAME, TP.SCHEMANAME, TP.NUM_OF_BIG_STRING_COLUMNS
        , TP.AVG_LENGTH_OF_BIG_STRING_COLUMNS, TP.NUM_OF_OTHER_BIG_COLUMNS, TP.NUM_OF_TOTAL_COLUMNS,TP.NUM_OF_DATE_COLUMNS
        , CSD.TABLE_PARTITION_COUNT as NUM_USER_PARTITIONS , CSD.ROW_COUNT_TOTAL
        , CSD.ROW_COUNT_PER_DISTRIBUTION_MAX, CSD.ROWGROUP_PER_DISTRIBUTION_MAX_IDEAL, CSD.INVISIBLE_ROWGROUP_COUNT
        , CSD.INVISIBLE_ROWGROUP_ROWS, CSD.INVISIBLE_ROWGROUP_ROWS_MIN, CSD.INVISIBLE_ROWGROUP_ROWS_MAX
        , CSD.INVISIBLE_ROWGROUP_ROWS_AVG, CSD.OPEN_ROWGROUP_COUNT, CSD.OPEN_ROWGROUP_ROWS, CSD.OPEN_ROWGROUP_ROWS_MIN
        , CSD.OPEN_ROWGROUP_ROWS_MAX, CSD.OPEN_ROWGROUP_ROWS_AVG, CSD.CLOSED_ROWGROUP_COUNT, CSD.CLOSED_ROWGROUP_ROWS
        , CSD.CLOSED_ROWGROUP_ROWS_MIN, CSD.CLOSED_ROWGROUP_ROWS_MAX, CSD.CLOSED_ROWGROUP_ROWS_AVG
        , CSD.COMPRESSED_ROWGROUP_COUNT, CSD.COMPRESSED_ROWGROUP_ROWS
        , CSD.COMPRESSED_ROWGROUP_ROWS_DELETED, CSD.COMPRESSED_ROWGROUP_ROWS_MIN
        , CSD.COMPRESSED_ROWGROUP_ROWS_MAX, CSD.COMPRESSED_ROWGROUP_ROWS_AVG, CSD.ROWGROUP_TOTAL_MAX_IDEAL
        , GRH.DICTIONARY_SIZE_TRIMMED_RG, GRH.DICTIONARY_SIZE_TRIMMED_RG_MAX_SIZE, GRH.DICTIONARY_SIZE_TRIMMED_RG_MIN_SIZE
        , GRH.DICTIONARY_SIZE_TRIMMED_RG_AVG_SIZE, GRH.BULKLOAD_TRIMMED_RG, GRH.BULKLOAD_TRIMMED_RG_MAX_SIZE
        , GRH.BULKLOAD_TRIMMED_RG_MIN_SIZE, GRH.BULKLOAD_TRIMMED_RG_AVG_SIZE, GRH.MEMORY_LIMITATION_TRIMMED_RG
        , GRH.MEMORY_LIMITATION_TRIMMED_RG_MAX_SIZE, GRH.MEMORY_LIMITATION_TRIMMED_RG_MIN_SIZE, GRH.MEMORY_LIMITATION_TRIMMED_RG_AVG_SIZE
        , DL.IS_EXTERNAL, DL.DISTRIBUTION_POLICY_DESC, DL.DISTCOL, DL.DISTCOL_DATATYPE, DL.STORAGETYPE, CASE WHEN ISNULL(CSD.TABLE_PARTITION_COUNT,1)>1 and DL.ISPARTITIONED=''YES'' then ''YES'' ELSE ''NO'' END as ISPARTITIONED, DL.NUMPARTITIONS NUM_SYSTEM_PARTITIONS, DL.NUMROWS
        , DSI.DISTR_MAX_ROW_COUNT, DSI.DISTR_MIN_ROW_COUNT, DSI.DISTR_AVG_ROW_COUNT, DSI.DISTR_MEDIAN_ROW_COUNT, DSI.DISTR_MAX_MIN_SKEW, DSI.DISTR_MAX_AVG_SKEW
        , DSI.DISTR_MAX_MED_SKEW
        , TSI.NEWEST_ACTIVE_STAT_DATE, TSI.OLDEST_ACTIVE_STAT_DATE, TSI.NUMBER_OF_STATS
        , TSI.NUM_OF_USERCREATED_STATS, TSI.NUM_OF_FILTERED_STATS
        FROM  '+@op_schema_name+'.HC_TABLE_PATTERN'+@tableRunTag+' TP 
            LEFT OUTER JOIN '+@op_schema_name+'.HC_COLUMN_STORE_DENSITY'+@tableRunTag+' CSD  ON TP.OBJECT_ID = CSD.OBJECT_ID
            LEFT OUTER JOIN '+@op_schema_name+'.HC_GENERAL_ROWGROUP_HEALTH'+@tableRunTag+' GRH ON TP.OBJECT_ID = GRH.OBJECT_ID
            LEFT OUTER JOIN '+@op_schema_name+'.HC_DISTRIBUTION_LAYOUT'+@tableRunTag+' DL  ON TP.OBJECT_ID = DL.OBJECT_ID
            LEFT OUTER JOIN '+@op_schema_name+'.HC_DISTRIBUTION_SKEW_INFO'+@tableRunTag+' DSI  ON TP.OBJECT_ID = DSI.OBJECT_ID
            LEFT OUTER JOIN '+@op_schema_name+'.HC_TABLE_STATS_INFO'+@tableRunTag+' TSI ON TP.OBJECT_ID = TSI.OBJECT_ID';


        --/**/PRINT(@queryText)  
        EXEC (@queryText) 
        --/**/PRINT(char(10)+'*********************************************************************************'+char(10))
        
        SET  @queryTargetTable = @op_schema_name+'.HC_SCORES_FLAGS'+@tableRunTag
        --*************************
        --all scores calculated are between 1 and 0, 
        -- 1 represents the worst,
        -- 0 represents the best condition for the scored property/behavior
        -- Some of the ratios are subtracted from 1 to fit into below conditions
        -- the scores and reverted versions of flags will be summed to calculate importance score on the next step.
        -- the less the merrier
        --*************************
        SET  @queryText =
            N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                DROP  TABLE '+@queryTargetTable+'
            CREATE TABLE '+@queryTargetTable+'
                WITH 
                    (
                        DISTRIBUTION = hash(TableName),
                        HEAP
                    )
            AS	 
            select 
            HC.HC_DATE,HC.TOTAL_ROW_COUNT,HC.OBJECT_ID,  HC.SCHEMANAME, HC.TABLENAME
            ,case when DATEDIFF(day,HC.OLDEST_ACTIVE_STAT_DATE,HC.NEWEST_ACTIVE_STAT_DATE)>0 or DATEDIFF(day,HC.NEWEST_ACTIVE_STAT_DATE,getdate())>0  then 1 else 0 end FLAG_STATS_LATE
            --**********
			,CAST(round(case when isnull(ROW_COUNT_PER_DISTRIBUTION_MAX,0)>0 then 1048576.0/HC.ROW_COUNT_PER_DISTRIBUTION_MAX else -1 end,4) AS DECIMAL(12,4)) as SCORE_TABLE_CCI_MATURITY
            ,case when isnull(HC.TOTAL_ROW_COUNT,0)>0 AND isnull(HC.TOTAL_ROW_COUNT,0)/(60.0)< 1048576*1.3 then 0 else 1 end as FLAG_TABLE_CCI_ELIGEBILITY
            ,case when isnull(HC.TOTAL_ROW_COUNT,0)>0 AND isnull(HC.NUM_USER_PARTITIONS,0)>1 and isnull(HC.TOTAL_ROW_COUNT,0)/(60.0*1.3)/isnull(HC.NUM_USER_PARTITIONS,0) < 1048576 then 0 else 1 end as FLAG_TABLE_CCI_PARTITION_HEALTH
            --*****************************************************************************************************************************************
            ,case when isnull(HC.TOTAL_ROW_COUNT,0)>0 AND isnull(HC.TOTAL_ROW_COUNT,0)/(60.0*1.3*1048576)>8 then 1 else 0 end  as FLAG_TABLE_PARTITION_ELIGEBILITY
            --*****************************************************************************************************************************************
            ,CAST(round(case when HC.STORAGETYPE=''CLUSTERED COLUMNSTORE'' AND isnull(HC.OPEN_ROWGROUP_COUNT,0)>60  then 1-60.0/HC.OPEN_ROWGROUP_COUNT 
                            when HC.STORAGETYPE=''CLUSTERED COLUMNSTORE'' AND isnull(HC.OPEN_ROWGROUP_COUNT,0)>0 then 0 else null end,4) AS DECIMAL(12,4)) as SCORE_SCORE_OPEN_RG
            ,CAST(round(case when HC.STORAGETYPE=''CLUSTERED COLUMNSTORE'' AND isnull(HC.COMPRESSED_ROWGROUP_COUNT,0)>0 then
                                case when  isnull(HC.ROWGROUP_TOTAL_MAX_IDEAL,0)>=60  and  isnull(HC.ROWGROUP_TOTAL_MAX_IDEAL,0)<=HC.COMPRESSED_ROWGROUP_COUNT
                                    then 1-(HC.ROWGROUP_TOTAL_MAX_IDEAL/(HC.COMPRESSED_ROWGROUP_COUNT*1.00)) 
                                    else 1  end
                                else null end,4) AS DECIMAL(12,4)) as SCORE_TABLE_CCI_EFFICIENCY
            ,CAST(round(case when HC.STORAGETYPE=''CLUSTERED COLUMNSTORE'' AND isnull(HC.COMPRESSED_ROWGROUP_COUNT,0)>0 then (HC.DICTIONARY_SIZE_TRIMMED_RG*1.00)/HC.COMPRESSED_ROWGROUP_COUNT else null end,4) AS DECIMAL(12,4)) as SCORE_TABLE_CCI_DICT_TRIMMIMG
            ,CAST(round(case when HC.STORAGETYPE=''CLUSTERED COLUMNSTORE'' AND isnull(HC.COMPRESSED_ROWGROUP_COUNT,0)>0 then (HC.BULKLOAD_TRIMMED_RG*1.00)/HC.COMPRESSED_ROWGROUP_COUNT else null end,4) AS DECIMAL(12,4)) as SCORE_TABLE_CCI_BULKLOAD_TRIMMIMG
            ,CAST(round(case when HC.STORAGETYPE=''CLUSTERED COLUMNSTORE'' AND isnull(HC.COMPRESSED_ROWGROUP_COUNT,0)>0 then (HC.MEMORY_LIMITATION_TRIMMED_RG*1.00)/HC.COMPRESSED_ROWGROUP_COUNT else null end,4) AS DECIMAL(12,4)) as SCORE_TABLE_CCI_MEMORY_TRIMMIMG
            --********************************************
            ,CAST(round(case when HC.STORAGETYPE=''CLUSTERED COLUMNSTORE''  then  (1048576 - HC.DICTIONARY_SIZE_TRIMMED_RG_AVG_SIZE)/1048576.0 else null end,4) AS DECIMAL(12,4)) as SCORE_TABLE_CCI_DICT_HEALTH
            ,CAST(round(case when HC.STORAGETYPE=''CLUSTERED COLUMNSTORE''  then (1048576 - HC.BULKLOAD_TRIMMED_RG_AVG_SIZE)/1048576.0  else null end,4) AS DECIMAL(12,4)) as SCORE_TABLE_CCI_BULKLOAD_HEALTH '+char(10)
            
            SET  @queryText2 =  N'            ,CAST(round(case when HC.STORAGETYPE=''CLUSTERED COLUMNSTORE''  then (1048576 - HC.MEMORY_LIMITATION_TRIMMED_RG_AVG_SIZE)/1048576.0  else null end,4) AS DECIMAL(12,4)) as SCORE_TABLE_CCI_MEMORY_HEALTH
            , CAST(round(case when HC.STORAGETYPE=''CLUSTERED COLUMNSTORE''  then (1048576 - HC.COMPRESSED_ROWGROUP_ROWS_AVG)/1048576.0  else null end,4) AS DECIMAL(12,4)) as SCORE_TABLE_CCI_COMPRESSED_HEALTH
            --********************************************
            ,   CAST(round(case when HC.STORAGETYPE=''CLUSTERED COLUMNSTORE'' and  isnull(HC.TOTAL_ROW_COUNT,0)>0  then HC.COMPRESSED_ROWGROUP_ROWS_DELETED*1.00/HC.TOTAL_ROW_COUNT else null end,4) AS DECIMAL(12,4)) as SCORE_DELETE
            , HC.ROW_COUNT_PER_DISTRIBUTION_MAX , HC.ROW_COUNT_TOTAL, HC.ROWGROUP_PER_DISTRIBUTION_MAX_IDEAL,HC.ROWGROUP_TOTAL_MAX_IDEAL, HC.NUMROWS
            , HC.STORAGETYPE, HC.DISTRIBUTION_POLICY_DESC, HC.DISTCOL, HC.DISTCOL_DATATYPE
            , HC.DISTR_AVG_ROW_COUNT, HC.DISTR_MEDIAN_ROW_COUNT,HC.DISTR_MAX_AVG_SKEW, HC.DISTR_MAX_MED_SKEW, HC.DISTR_MAX_MIN_SKEW, HC.DISTR_MAX_ROW_COUNT,  HC.DISTR_MIN_ROW_COUNT
            , HC.OPEN_ROWGROUP_COUNT, HC.OPEN_ROWGROUP_ROWS, HC.OPEN_ROWGROUP_ROWS_AVG, HC.OPEN_ROWGROUP_ROWS_MAX, HC.OPEN_ROWGROUP_ROWS_MIN
            , HC.CLOSED_ROWGROUP_COUNT, HC.CLOSED_ROWGROUP_ROWS, HC.CLOSED_ROWGROUP_ROWS_AVG, HC.CLOSED_ROWGROUP_ROWS_MAX, HC.CLOSED_ROWGROUP_ROWS_MIN
            , HC.COMPRESSED_ROWGROUP_COUNT, HC.COMPRESSED_ROWGROUP_ROWS, HC.COMPRESSED_ROWGROUP_ROWS_AVG, HC.COMPRESSED_ROWGROUP_ROWS_DELETED, HC.COMPRESSED_ROWGROUP_ROWS_MAX, HC.COMPRESSED_ROWGROUP_ROWS_MIN
            , HC.MEMORY_LIMITATION_TRIMMED_RG, HC.MEMORY_LIMITATION_TRIMMED_RG_AVG_SIZE, HC.MEMORY_LIMITATION_TRIMMED_RG_MAX_SIZE, HC.MEMORY_LIMITATION_TRIMMED_RG_MIN_SIZE
            , HC.BULKLOAD_TRIMMED_RG, HC.BULKLOAD_TRIMMED_RG_AVG_SIZE, HC.BULKLOAD_TRIMMED_RG_MAX_SIZE, HC.BULKLOAD_TRIMMED_RG_MIN_SIZE
            , HC.DICTIONARY_SIZE_TRIMMED_RG, HC.DICTIONARY_SIZE_TRIMMED_RG_AVG_SIZE, HC.DICTIONARY_SIZE_TRIMMED_RG_MAX_SIZE, HC.DICTIONARY_SIZE_TRIMMED_RG_MIN_SIZE
            , HC.NUM_OF_BIG_STRING_COLUMNS, HC.AVG_LENGTH_OF_BIG_STRING_COLUMNS, HC.NUM_OF_OTHER_BIG_COLUMNS, HC.NUM_OF_TOTAL_COLUMNS, HC.NUM_OF_DATE_COLUMNS
            , HC.INVISIBLE_ROWGROUP_COUNT, HC.INVISIBLE_ROWGROUP_ROWS, HC.INVISIBLE_ROWGROUP_ROWS_AVG, HC.INVISIBLE_ROWGROUP_ROWS_MAX, HC.INVISIBLE_ROWGROUP_ROWS_MIN
            , HC.IS_EXTERNAL, HC.ISPARTITIONED, HC.NUM_SYSTEM_PARTITIONS, HC.NUM_USER_PARTITIONS
            , HC.NEWEST_ACTIVE_STAT_DATE,  HC.OLDEST_ACTIVE_STAT_DATE, HC.NUM_OF_FILTERED_STATS, HC.NUM_OF_USERCREATED_STATS, HC.NUMBER_OF_STATS  
            from '+@op_schema_name+'.HC_BASE'+@tableRunTag+' HC'


        --/**/PRINT(@queryText)  
		--/**/PRINT(@queryText2)
        EXEC (@queryText + @queryText2) 
        --/**/PRINT(char(10)+'*********************************************************************************'+char(10))
        
        SET  @queryTargetTable = @op_schema_name+'.HC_WITH_IMPORTANCE'+@tableRunTag
                --*************************
        -- here we calculate the Importance Score
        -- the scores and reverted versions of flags will be summed to calculate importance score.
        -- Importance Score is used to mark the tables which needs more/immediate attention.
        -- Importance Score Range is:
        -- Max: 12 Min:0
        -- the less the merrier but unless it is less than 1 there is always an issue that can/needs to be adressed.
        -- Start from the tables having highest importance score and  is creating a bottleneck in the ETL flow, getting read frequently by subsequent 
        --*************************
        SET  @queryText =
            N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                DROP  TABLE '+@queryTargetTable+'
            CREATE TABLE '+@queryTargetTable+'
                WITH 
                    (
                        DISTRIBUTION = hash(TableName),
                        HEAP
                    )
            AS	 
            select 
            HC.HC_DATE, format(HC.TOTAL_ROW_COUNT ,''#,#'') as TOTAL_ROW_COUNT
            , ( ISNULL(HC.SCORE_SCORE_OPEN_RG,0) + ISNULL(HC.SCORE_TABLE_CCI_BULKLOAD_HEALTH,0) + ISNULL(HC.SCORE_TABLE_CCI_BULKLOAD_TRIMMIMG,0) 
            + ISNULL(HC.SCORE_TABLE_CCI_COMPRESSED_HEALTH,0) + ISNULL(HC.SCORE_TABLE_CCI_DICT_HEALTH,0) + ISNULL(HC.SCORE_TABLE_CCI_DICT_TRIMMIMG,0) + ISNULL(HC.SCORE_TABLE_CCI_EFFICIENCY,0) 
            + ISNULL(HC.SCORE_TABLE_CCI_MEMORY_HEALTH,0) + ISNULL(HC.SCORE_TABLE_CCI_MEMORY_TRIMMIMG,0) + ISNULL(HC.SCORE_DELETE,0)
            + CASE WHEN [FLAG_TABLE_CCI_ELIGEBILITY]=1 and  [STORAGETYPE]!=''CLUSTERED COLUMNSTORE'' and [IS_EXTERNAL]!=1 THEN 1 ELSE 0 END
            + CASE WHEN FLAG_TABLE_CCI_PARTITION_HEALTH=0 and FLAG_TABLE_PARTITION_ELIGEBILITY=1 THEN 1 ELSE 0 end) as IMPORTANCE
            , HC.OBJECT_ID,  HC.SCHEMANAME, HC.TABLENAME
            , HC.FLAG_STATS_LATE, HC.FLAG_TABLE_CCI_ELIGEBILITY, HC.FLAG_TABLE_CCI_PARTITION_HEALTH, HC.FLAG_TABLE_PARTITION_ELIGEBILITY
            , HC.ROW_COUNT_PER_DISTRIBUTION_MAX , HC.ROW_COUNT_TOTAL
            , HC.SCORE_SCORE_OPEN_RG, HC.SCORE_TABLE_CCI_BULKLOAD_HEALTH, HC.SCORE_TABLE_CCI_BULKLOAD_TRIMMIMG, HC.SCORE_TABLE_CCI_COMPRESSED_HEALTH
            , HC.SCORE_DELETE
            , HC.SCORE_TABLE_CCI_DICT_HEALTH, HC.SCORE_TABLE_CCI_DICT_TRIMMIMG, HC.SCORE_TABLE_CCI_EFFICIENCY, HC.SCORE_TABLE_CCI_MEMORY_HEALTH
            , HC.SCORE_TABLE_CCI_MEMORY_TRIMMIMG
            , HC.ROWGROUP_PER_DISTRIBUTION_MAX_IDEAL,HC.ROWGROUP_TOTAL_MAX_IDEAL
            , HC.STORAGETYPE, HC.DISTRIBUTION_POLICY_DESC, HC.DISTCOL, HC.DISTCOL_DATATYPE
            , HC.DISTR_AVG_ROW_COUNT, HC.DISTR_MEDIAN_ROW_COUNT,HC.DISTR_MAX_AVG_SKEW, HC.DISTR_MAX_MED_SKEW, HC.DISTR_MAX_MIN_SKEW, HC.DISTR_MAX_ROW_COUNT,  HC.DISTR_MIN_ROW_COUNT
            , HC.OPEN_ROWGROUP_COUNT, HC.OPEN_ROWGROUP_ROWS, HC.OPEN_ROWGROUP_ROWS_AVG, HC.OPEN_ROWGROUP_ROWS_MAX, HC.OPEN_ROWGROUP_ROWS_MIN
            , HC.CLOSED_ROWGROUP_COUNT, HC.CLOSED_ROWGROUP_ROWS, HC.CLOSED_ROWGROUP_ROWS_AVG, HC.CLOSED_ROWGROUP_ROWS_MAX, HC.CLOSED_ROWGROUP_ROWS_MIN
            , HC.COMPRESSED_ROWGROUP_COUNT, HC.COMPRESSED_ROWGROUP_ROWS, HC.COMPRESSED_ROWGROUP_ROWS_AVG, HC.COMPRESSED_ROWGROUP_ROWS_DELETED, HC.COMPRESSED_ROWGROUP_ROWS_MAX, HC.COMPRESSED_ROWGROUP_ROWS_MIN
            , HC.MEMORY_LIMITATION_TRIMMED_RG, HC.MEMORY_LIMITATION_TRIMMED_RG_AVG_SIZE, HC.MEMORY_LIMITATION_TRIMMED_RG_MAX_SIZE, HC.MEMORY_LIMITATION_TRIMMED_RG_MIN_SIZE
            , HC.BULKLOAD_TRIMMED_RG, HC.BULKLOAD_TRIMMED_RG_AVG_SIZE, HC.BULKLOAD_TRIMMED_RG_MAX_SIZE, HC.BULKLOAD_TRIMMED_RG_MIN_SIZE
            , HC.DICTIONARY_SIZE_TRIMMED_RG, HC.DICTIONARY_SIZE_TRIMMED_RG_AVG_SIZE, HC.DICTIONARY_SIZE_TRIMMED_RG_MAX_SIZE, HC.DICTIONARY_SIZE_TRIMMED_RG_MIN_SIZE
            , HC.NUM_OF_BIG_STRING_COLUMNS, HC.AVG_LENGTH_OF_BIG_STRING_COLUMNS, HC.NUM_OF_OTHER_BIG_COLUMNS, HC.NUM_OF_TOTAL_COLUMNS, HC.NUM_OF_DATE_COLUMNS 
            , HC.INVISIBLE_ROWGROUP_COUNT, HC.INVISIBLE_ROWGROUP_ROWS, HC.INVISIBLE_ROWGROUP_ROWS_AVG, HC.INVISIBLE_ROWGROUP_ROWS_MAX, HC.INVISIBLE_ROWGROUP_ROWS_MIN
            , HC.IS_EXTERNAL,  HC.ISPARTITIONED, HC.NUM_SYSTEM_PARTITIONS, HC.NUM_USER_PARTITIONS,HC.NUMROWS
            , HC.NEWEST_ACTIVE_STAT_DATE,  HC.OLDEST_ACTIVE_STAT_DATE, HC.NUM_OF_FILTERED_STATS, HC.NUM_OF_USERCREATED_STATS, HC.NUMBER_OF_STATS
            from '+@op_schema_name+'.HC_SCORES_FLAGS'+@tableRunTag+'  as HC '


        --/**/PRINT(@queryText)  
        EXEC (@queryText) 
        --/**/PRINT(char(10)+'*********************************************************************************'+char(10))
        
       
        IF @report_type = 'CTAS'
        BEGIN
            SET  @queryTargetTable = @op_schema_name+'.HC_REPORT'+@tableRunTag
            SET  @queryText =  					N'                    IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                    DROP  TABLE '+@queryTargetTable+'
					CREATE TABLE '+@queryTargetTable+'
						WITH 
							(
								DISTRIBUTION = hash(TableName),
								HEAP
							)
					AS	 '
        END
        ELSE  /* @report_type = 'INSERT' */
        BEGIN
            SET  @queryTargetTable = @op_schema_name+'.HC_REPORT'
            IF OBJECT_ID(@queryTargetTable) IS NULL
               BEGIN
                    SET  @queryText = N'                            CREATE TABLE '+@queryTargetTable+'
                                WITH 
                                    (   DISTRIBUTION = hash(TableName),
                                        HEAP  )   AS '+char(10)
                END
            ELSE
            BEGIN
					SET  @queryText = N'                INSERT INTO '+@queryTargetTable+'
					([HC_DATE], [TOTAL_ROW_COUNT], [IMPORTANCE], [REPORT_COMMENT], [OBJECT_ID], [SCHEMANAME], [TABLENAME], [FLAG_STATS_LATE]
                    ,[FLAG_TABLE_CCI_ELIGEBILITY], [FLAG_TABLE_CCI_PARTITION_HEALTH], [FLAG_TABLE_PARTITION_ELIGEBILITY], [ROW_COUNT_PER_DISTRIBUTION_MAX]
                    ,[ROW_COUNT_TOTAL], [SCORE_SCORE_OPEN_RG], [SCORE_TABLE_CCI_BULKLOAD_HEALTH], [SCORE_TABLE_CCI_BULKLOAD_TRIMMIMG], [SCORE_TABLE_CCI_COMPRESSED_HEALTH]
                    ,[SCORE_DELETE], [SCORE_TABLE_CCI_DICT_HEALTH], [SCORE_TABLE_CCI_DICT_TRIMMIMG], [SCORE_TABLE_CCI_EFFICIENCY], [SCORE_TABLE_CCI_MEMORY_HEALTH]
                    ,[SCORE_TABLE_CCI_MEMORY_TRIMMIMG], [ROWGROUP_PER_DISTRIBUTION_MAX_IDEAL], [ROWGROUP_TOTAL_MAX_IDEAL], [STORAGETYPE], [DISTRIBUTION_POLICY_DESC]
                    ,[DISTCOL], [DISTCOL_DATATYPE], [DISTR_AVG_ROW_COUNT], [DISTR_MEDIAN_ROW_COUNT], [DISTR_MAX_AVG_SKEW], [DISTR_MAX_MED_SKEW], [DISTR_MAX_MIN_SKEW]
                    ,[DISTR_MAX_ROW_COUNT], [DISTR_MIN_ROW_COUNT], [OPEN_ROWGROUP_COUNT], [OPEN_ROWGROUP_ROWS], [OPEN_ROWGROUP_ROWS_AVG], [OPEN_ROWGROUP_ROWS_MAX]
                    ,[OPEN_ROWGROUP_ROWS_MIN], [CLOSED_ROWGROUP_COUNT], [CLOSED_ROWGROUP_ROWS], [CLOSED_ROWGROUP_ROWS_AVG], [CLOSED_ROWGROUP_ROWS_MAX], [CLOSED_ROWGROUP_ROWS_MIN]
                    ,[COMPRESSED_ROWGROUP_COUNT], [COMPRESSED_ROWGROUP_ROWS], [COMPRESSED_ROWGROUP_ROWS_AVG], [COMPRESSED_ROWGROUP_ROWS_DELETED], [COMPRESSED_ROWGROUP_ROWS_MAX]
                    ,[COMPRESSED_ROWGROUP_ROWS_MIN], [MEMORY_LIMITATION_TRIMMED_RG], [MEMORY_LIMITATION_TRIMMED_RG_AVG_SIZE], [MEMORY_LIMITATION_TRIMMED_RG_MAX_SIZE]
                    ,[MEMORY_LIMITATION_TRIMMED_RG_MIN_SIZE], [BULKLOAD_TRIMMED_RG], [BULKLOAD_TRIMMED_RG_AVG_SIZE], [BULKLOAD_TRIMMED_RG_MAX_SIZE], [BULKLOAD_TRIMMED_RG_MIN_SIZE]
                    ,[DICTIONARY_SIZE_TRIMMED_RG], [DICTIONARY_SIZE_TRIMMED_RG_AVG_SIZE], [DICTIONARY_SIZE_TRIMMED_RG_MAX_SIZE], [DICTIONARY_SIZE_TRIMMED_RG_MIN_SIZE]
                    ,[NUM_OF_BIG_STRING_COLUMNS], [AVG_LENGTH_OF_BIG_STRING_COLUMNS], [NUM_OF_OTHER_BIG_COLUMNS], [NUM_OF_TOTAL_COLUMNS], [NUM_OF_DATE_COLUMNS]
                    ,[INVISIBLE_ROWGROUP_COUNT], [INVISIBLE_ROWGROUP_ROWS], [INVISIBLE_ROWGROUP_ROWS_AVG], [INVISIBLE_ROWGROUP_ROWS_MAX], [INVISIBLE_ROWGROUP_ROWS_MIN]
                    ,[IS_EXTERNAL], [ISPARTITIONED], [NUM_SYSTEM_PARTITIONS], [NUM_USER_PARTITIONS], [NUMROWS], [NEWEST_ACTIVE_STAT_DATE], [OLDEST_ACTIVE_STAT_DATE]
                    ,[NUM_OF_FILTERED_STATS], [NUM_OF_USERCREATED_STATS], [NUMBER_OF_STATS]) '+char(10)
			END
        END
        ----------------
        SET  @queryText = @queryText    +'select 
            HC.HC_DATE, HC.TOTAL_ROW_COUNT , HC.IMPORTANCE
            , case when HC.STORAGETYPE =''CLUSTERED COLUMNSTORE'' 
                    then 
                        case   when  ISNULL(HC.SCORE_SCORE_OPEN_RG,0)>0 and HC.FLAG_TABLE_CCI_ELIGEBILITY=1
                            THEN  ''There are too many OPEN Row groups in this table, this may happen because of too many small sized batch or singleton  INSERTS or UPDATES, or high MAXDOP. Try reducing MAXDOP or implement DROP CTAS, ''
                                    + case when HC.FLAG_TABLE_PARTITION_ELIGEBILITY=1 
                                        then ''or if there is a suitable'' + case when ISNULL(HC.NUM_OF_DATE_COLUMNS,0) >0 then '' incremental DATE type of'' else '''' end + '' column, implement PARTITION SWITCH ''
                                        else ''''
                                    end 
                                    +''logic. ''
                            else ''''
                        end
                        + case when HC.FLAG_TABLE_CCI_ELIGEBILITY=0 '+char(10)
        SET  @queryText2 =
            N'                         then UPPER(HC.tablename)+'' table does not have enough records to have a successful CCI index, other indexing[storage] methodologies like HEAP or CLUSTERED INDEX (if queries hitting the table has order based window functions like LEAD, LAG, PARTITION BY, RANK etc. by indexing column candidate) ''
                                +''with secondary indexes on frequently filtered columns should be implemented. ''
                                +''REPLICATED can be a good distribution strategy for these kind of tables if they are used as DIMENSIONs and joined heavily with facts by subsequent processes or ad-hoc queries. ''
                            when HC.SCORE_TABLE_CCI_EFFICIENCY > 0.3
                            then  UPPER(HC.tablename)+'' has a very low quality CCI Row Group health. ''--+char(10)
                                    +''The compressed row groups are '' + TRIM(str(HC.SCORE_TABLE_CCI_COMPRESSED_HEALTH*100))+char(37)+'' unutilized comparing to maximum possible. ''
                                    /*Dictionary Trimming*/
                                    + CASE when HC.SCORE_TABLE_CCI_DICT_HEALTH > 0.3 and HC.SCORE_TABLE_CCI_DICT_TRIMMIMG > 0.1 
                                            then TRIM(str(100*HC.SCORE_TABLE_CCI_DICT_TRIMMIMG))+char(37)+'' of the compressed row groups are DICTIONARY TRIMMED in average of '' 
                                                + case when HC.DICTIONARY_SIZE_TRIMMED_RG_AVG_SIZE>1000.0 
                                                    then TRIM(STR(ROUND(HC.DICTIONARY_SIZE_TRIMMED_RG_AVG_SIZE/1000.0,0)))+''K''
                                                    else TRIM(STR(HC.DICTIONARY_SIZE_TRIMMED_RG_AVG_SIZE)) 
                                                end
                                                + '' rows per rowgroup. ''   
                                                + case when ISNULL(hc.NUM_OF_BIG_STRING_COLUMNS,0)+ISNULL(hc.NUM_OF_OTHER_BIG_COLUMNS,0)>0
                                                    then ''The table has '' +case when ISNULL(hc.NUM_OF_BIG_STRING_COLUMNS,0)>0                                                                                 
                                                                              then TRIM(str(hc.NUM_OF_BIG_STRING_COLUMNS)) +'' big string columns with in average ''+TRIM(str(HC.AVG_LENGTH_OF_BIG_STRING_COLUMNS))
                                                                                    +'' bytes length and '' 
                                                                            else ''''
                                                                            end               
                                                                            +case when ISNULL(hc.NUM_OF_OTHER_BIG_COLUMNS,0)>0 
                                                                                then TRIM(str(hc.NUM_OF_OTHER_BIG_COLUMNS))+'' big numeric or date columns;''
                                                                                else ''''
                                                                            end
                                                            +''to avoid DICTIONARY TRIMMING try to reduce the size of any existing unnecessarily big text columns, especially bigger than 32 bytes, and  reduce the size/number of unnecessarily big columns, especially bigint types. ''
                                                        else ''''
                                                end
                                            else ''''
                                        END '+char(10)
                                    /*Bulkload Trimming*/
             SET  @queryText3 =
            N'                                    + CASE when HC.SCORE_TABLE_CCI_BULKLOAD_HEALTH > 0.3 and HC.SCORE_TABLE_CCI_BULKLOAD_TRIMMIMG > 0.1 and BULKLOAD_TRIMMED_RG> 60
                                            then TRIM(str(100*HC.SCORE_TABLE_CCI_BULKLOAD_TRIMMIMG))+char(37)+'' of the compressed row groups are BULKLOAD TRIMMED in average of '' 
                                                + case when HC.BULKLOAD_TRIMMED_RG_AVG_SIZE>1000.0 
                                                    then TRIM(STR(ROUND(HC.BULKLOAD_TRIMMED_RG_AVG_SIZE/1000.0,0)))+''K ''
                                                    else TRIM(STR(HC.BULKLOAD_TRIMMED_RG_AVG_SIZE)) 
                                                end
                                                + ''rows per rowgourp.''
                                                + ''BULKLOAD TRIMMING happens when the batch data size is not big enough (1M) in loads, or the batch_row_count%1M per distribution has some remaining rows that can not be compressed withouth trimming or MAXDOP is too high.''
                                                + Case when HC.ISPARTITIONED =''YES''  AND ISNULL(HC.NUM_USER_PARTITIONS,0)>1  and HC.FLAG_TABLE_CCI_PARTITION_HEALTH=0
                                                    THEN ''Table has ''+TRIM(STR(HC.NUM_USER_PARTITIONS))
                                                        +'' active data containing partitions, which is too many for the table current data size, casuing low CCI health.''
                                                    else ''''
                                                end
                                                + Case when HC.DISTR_MAX_MED_SKEW >0.1 and HC.DISTR_MAX_MIN_SKEW>0.15
                                                    THEN ''Table has ''+TRIM(STR(HC.DISTR_MAX_MIN_SKEW*100))+char(37)
                                                        +'' SKEW between MAX and MIN number of row containing distributions, SKEW can also be another reason for BULKLOAD TRIMMING''
                                                        + case when ISNULL(HC.DISTRIBUTION_POLICY_DESC,''NONE'') =''HASH'' and lower(HC.DISTCOL_DATATYPE) not like ''%date%'' 
                                                                then ''If suitable changing the distribution key from ''+HC.DISTCOL+'' to another column may be considered, but for that the effect in reading workloads should be examined''
                                                                when lower(ISNULL(HC.DISTCOL_DATATYPE,''none'')) like ''%date%'' 
                                                                then ''the distribution column ''+HC.DISTCOL+'' is in type ''+upper(HC.DISTCOL_DATATYPE)+'' which is  not adviced at all. ''+upper(HC.DISTCOL_DATATYPE)+'' type columns cause usually skew and and data layout imbalance.''
                                                                    +''Change the distiribution column to a suitable ID column, which is used in joins reading the table. You can use ''+HC.DISTCOL+'' as partitioning column if data size is suitable and data is received in an  incremental pattern by this column.''
                                                                 else ''''
                                                            end
                                                            else ''''
                                                end
                                            else ''''
                                    end' +char(10)
                                    /*Memory Trimming*/
            SET  @queryText4 = N'                                   + CASE when HC.SCORE_TABLE_CCI_MEMORY_HEALTH > 0.3 and HC.SCORE_TABLE_CCI_MEMORY_TRIMMIMG > 0.1
                                            then TRIM(str(100*HC.SCORE_TABLE_CCI_MEMORY_TRIMMIMG))+char(37)+'' of the compressed row groups are MEMORY TRIMMED in average of '' 
                                                + case when HC.MEMORY_LIMITATION_TRIMMED_RG_AVG_SIZE>1000.0 
                                                    then TRIM(STR(ROUND(HC.MEMORY_LIMITATION_TRIMMED_RG_AVG_SIZE/1000.0,0)))+''K ''
                                                    else TRIM(STR(HC.MEMORY_LIMITATION_TRIMMED_RG_AVG_SIZE)) 
                                                end
                                                + ''rows per rowgourp.''
                                                + ''MEMORY LIMITATION TRIMMING happens when the distribution does not have enough resources at the time of executin to compress all incoming batch data for that distribution.''
                                                + case when hc.NUM_OF_BIG_STRING_COLUMNS>0 and HC.AVG_LENGTH_OF_BIG_STRING_COLUMNS>32
                                                    then ''The table has '' +TRIM(str(hc.NUM_OF_BIG_STRING_COLUMNS))+'' big string columns with in average ''+TRIM(str(HC.AVG_LENGTH_OF_BIG_STRING_COLUMNS))+'' bytes length.''--+char(10) 
                                                            +''Every string column bigger than 32 bytes requires 16 MB extra memory for compressing the data to CCI.''
                                                            +''To avoid MEMORY LIMITATION TRIMMING try to reduce the size/number of unnecessarily bigger than 32 bytes string.''
                                                        else ''''
                                                end
                                            else ''''
                                    end
                                    + CASE when ISNULL(HC.DISTRIBUTION_POLICY_DESC,''NONE'') = ''ROUND_ROBIN'' 
                                        then ''Detect a suitable distribution key resulting less then 10% MAX-MEDIAN/AVG SKEW which is used in joins reading the table or in alignment with the tables sourcing this table, and change the distribution methodology to HASH.'' 
                                        else ''''
                                    end 
                                WHEN ISNULL(HC.DISTRIBUTION_POLICY_DESC,''NONE'') = ''ROUND_ROBIN'' and HC.FLAG_TABLE_CCI_ELIGEBILITY=1
                                then ''Detect a suitable distribution key resulting less then 10% MAX-MEDIAN/AVG SKEW which is used in joins reading the table or in alignment with the tables sourcing this table, and change the distribution methodology to HASH.'' 
                                WHEN HC.FLAG_TABLE_PARTITION_ELIGEBILITY=1 
                                then ''Data size is suitable for implementing partitioning column if the data is received in an incremental pattern by a DATE type family column. By implementing incremental load with PARTITION SWITCHING, LOAD times can be dramatically improved. ''
                                else ''''
                        END
                    when HC.STORAGETYPE =''HEAP'' 
                    then
                        case  when HC.FLAG_TABLE_CCI_ELIGEBILITY=1 and  ISNULL(HC.DISTRIBUTION_POLICY_DESC,''NONE'') = ''ROUND_ROBIN''          
                                then ''Convert the table strorage [primary indexing] to CCI if the table is read by subsequent processes and/or scheduled /ad-hoc reports & queries.''
                                +''Detect a suitable distribution key resulting less then 10% MAX-MEDIAN/AVG SKEW, which is used in JOINs reading the table or in alignment with the tables sourcing this table, and change the distribution methodology to HASH.'' '+char(10)
            SET  @queryText5 =
            N'                            when HC.FLAG_TABLE_CCI_ELIGEBILITY=1 and  ISNULL(HC.DISTRIBUTION_POLICY_DESC,''NONE'') = ''HASH'' 
                            then ''Convert the table strorage [primary indexing] to CCI with the same or a more suitable HASH key, if the table is read by subsequent processes and/or scheduled/ad-hoc reports & queries.''
                            else ''''
                        end 
                    when HC.STORAGETYPE =''CLUSTERED INDEX'' and HC.FLAG_TABLE_CCI_ELIGEBILITY=1
                    then ''If there is no specific reason to keep the data ordered by the  ''+HC.DISTCOL+'' then convert the index to CCI.''
                    when HC.STORAGETYPE =''CLUSTERED INDEX'' and HC.FLAG_TABLE_CCI_ELIGEBILITY=0
                    then ''If there is no specific reason to keep the data ordered by the  ''+HC.DISTCOL+'' like querying the table with order based window functions like LEAD, LAG, PARTITION BY, RANK, etc., then converting the storage/index to HEAP can improve the load time.''
                    else ''''
                end 
                + case when  HC.DISTR_MAX_MED_SKEW >0.1 and HC.DISTR_MAX_MIN_SKEW>0.15 and HC.FLAG_TABLE_CCI_ELIGEBILITY=1
                    THEN ''Table has ''+TRIM(STR(HC.DISTR_MAX_MIN_SKEW*100))+char(37) +'' MAX_MIN_SKEW between MAX and MIN number of row containing distributions and ''+TRIM(STR(HC.DISTR_MAX_MED_SKEW*100))+char(37) 
                        +'' MAX_MEDIAN_SKEW between MAX and MEDIAN number of row containing distributions. ''
                        +''This means any query running on this table waits for the MAX row containing distribution to finish approximately ''+TRIM(STR(HC.DISTR_MAX_MED_SKEW*100))+char(37)+'' longer, although around more than 50% of the distributions have already finished.'' 
                    else ''''
                end
                + case when HC.FLAG_STATS_LATE = 1 
                    THEN ''Table has ''+TRIM(STR(HC.DISTR_MAX_MIN_SKEW*100))+char(37) +'' MAX_MIN_SKEW between MAX and MIN number of row containing distributions and ''+TRIM(STR(HC.DISTR_MAX_MED_SKEW*100))+char(37) 
                        +'' MAX_MEDIAN_SKEW between MAX and MEDIAN number of row containing distributions. '' 
                        +''This means any query running on this table waits for the MAX row containing distribution to finish approximately ''+TRIM(STR(HC.DISTR_MAX_MED_SKEW*100))+char(37)+'' longer, although around more than 50% of the distributions have already finished.'' 
                    else ''''
                end
                + case when  HC.SCORE_DELETE >0 
                    THEN ''DELETE or UPDATE operations are running on this table, which slows down also the query performance. Implement DROP CTAS, '' 
                         + case when HC.FLAG_TABLE_PARTITION_ELIGEBILITY=1 
                                then ''or if there is a suitable incremental DATE type of column,set based PARTITION SWITCH ''
                                else ''''
                            end
                            +''logic with using JOINS to substiture UPDATES and DELETES to prevent DELETE BITMAP growth.  ''
                    else ''''
                end     as REPORT_COMMENT '+char(10)
            SET  @queryText6 =
            N'            , HC.OBJECT_ID,  HC.SCHEMANAME, HC.TABLENAME
            , HC.FLAG_STATS_LATE, HC.FLAG_TABLE_CCI_ELIGEBILITY, HC.FLAG_TABLE_CCI_PARTITION_HEALTH,HC.FLAG_TABLE_PARTITION_ELIGEBILITY
            , HC.ROW_COUNT_PER_DISTRIBUTION_MAX , HC.ROW_COUNT_TOTAL , HC.SCORE_SCORE_OPEN_RG, HC.SCORE_TABLE_CCI_BULKLOAD_HEALTH, HC.SCORE_TABLE_CCI_BULKLOAD_TRIMMIMG
            , HC.SCORE_TABLE_CCI_COMPRESSED_HEALTH, HC.SCORE_DELETE, HC.SCORE_TABLE_CCI_DICT_HEALTH, HC.SCORE_TABLE_CCI_DICT_TRIMMIMG, HC.SCORE_TABLE_CCI_EFFICIENCY
            , HC.SCORE_TABLE_CCI_MEMORY_HEALTH, HC.SCORE_TABLE_CCI_MEMORY_TRIMMIMG, HC.ROWGROUP_PER_DISTRIBUTION_MAX_IDEAL,HC.ROWGROUP_TOTAL_MAX_IDEAL
            , HC.STORAGETYPE, HC.DISTRIBUTION_POLICY_DESC, HC.DISTCOL, HC.DISTCOL_DATATYPE
            , HC.DISTR_AVG_ROW_COUNT, HC.DISTR_MEDIAN_ROW_COUNT,HC.DISTR_MAX_AVG_SKEW, HC.DISTR_MAX_MED_SKEW, HC.DISTR_MAX_MIN_SKEW, HC.DISTR_MAX_ROW_COUNT,  HC.DISTR_MIN_ROW_COUNT
            , HC.OPEN_ROWGROUP_COUNT, HC.OPEN_ROWGROUP_ROWS, HC.OPEN_ROWGROUP_ROWS_AVG, HC.OPEN_ROWGROUP_ROWS_MAX, HC.OPEN_ROWGROUP_ROWS_MIN
            , HC.CLOSED_ROWGROUP_COUNT, HC.CLOSED_ROWGROUP_ROWS, HC.CLOSED_ROWGROUP_ROWS_AVG, HC.CLOSED_ROWGROUP_ROWS_MAX, HC.CLOSED_ROWGROUP_ROWS_MIN
            , HC.COMPRESSED_ROWGROUP_COUNT, HC.COMPRESSED_ROWGROUP_ROWS, HC.COMPRESSED_ROWGROUP_ROWS_AVG, HC.COMPRESSED_ROWGROUP_ROWS_DELETED, HC.COMPRESSED_ROWGROUP_ROWS_MAX, HC.COMPRESSED_ROWGROUP_ROWS_MIN
            , HC.MEMORY_LIMITATION_TRIMMED_RG, HC.MEMORY_LIMITATION_TRIMMED_RG_AVG_SIZE, HC.MEMORY_LIMITATION_TRIMMED_RG_MAX_SIZE, HC.MEMORY_LIMITATION_TRIMMED_RG_MIN_SIZE
            , HC.BULKLOAD_TRIMMED_RG, HC.BULKLOAD_TRIMMED_RG_AVG_SIZE, HC.BULKLOAD_TRIMMED_RG_MAX_SIZE, HC.BULKLOAD_TRIMMED_RG_MIN_SIZE
            , HC.DICTIONARY_SIZE_TRIMMED_RG, HC.DICTIONARY_SIZE_TRIMMED_RG_AVG_SIZE, HC.DICTIONARY_SIZE_TRIMMED_RG_MAX_SIZE, HC.DICTIONARY_SIZE_TRIMMED_RG_MIN_SIZE
            , HC.NUM_OF_BIG_STRING_COLUMNS, HC.AVG_LENGTH_OF_BIG_STRING_COLUMNS, HC.NUM_OF_OTHER_BIG_COLUMNS, HC.NUM_OF_TOTAL_COLUMNS , HC.NUM_OF_DATE_COLUMNS
            , HC.INVISIBLE_ROWGROUP_COUNT, HC.INVISIBLE_ROWGROUP_ROWS, HC.INVISIBLE_ROWGROUP_ROWS_AVG, HC.INVISIBLE_ROWGROUP_ROWS_MAX, HC.INVISIBLE_ROWGROUP_ROWS_MIN
            , HC.IS_EXTERNAL, HC.ISPARTITIONED, HC.NUM_SYSTEM_PARTITIONS, ISNULL(HC.NUM_USER_PARTITIONS,1) as NUM_USER_PARTITIONS, HC.NUMROWS
            , HC.NEWEST_ACTIVE_STAT_DATE,  HC.OLDEST_ACTIVE_STAT_DATE, HC.NUM_OF_FILTERED_STATS, HC.NUM_OF_USERCREATED_STATS, HC.NUMBER_OF_STATS
            from '+@op_schema_name+'.HC_WITH_IMPORTANCE'+@tableRunTag+'  as HC '

        --/**/PRINT(@queryText)  
		--/**/PRINT(@queryText2)  
		--/**/PRINT(@queryText3)
		--/**/PRINT(@queryText4)  
        --/**/PRINT(@queryText5)
        --/**/PRINT(@queryText6)
        EXEC (@queryText + @queryText2 + @queryText3 + @queryText4 + @queryText5 + @queryText6) 
--**********************************************************************
-- Below code drops staging tables.
-- 
--**********************************************************************
        IF  @stage_cleanse_type ='DROP'
        BEGIN
            SET  @queryTargetTable = @op_schema_name+'.HC_TABLE_PATTERN'+@tableRunTag
            SET  @queryText =
                N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                    DROP  TABLE '+@queryTargetTable
            EXEC (@queryText)
            ---------------------
            SET  @queryTargetTable = @op_schema_name+'.HC_COLUMN_STORE_DENSITY'+@tableRunTag
            SET  @queryText =
                N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                    DROP  TABLE '+@queryTargetTable
            EXEC (@queryText)
            ---------------------
            SET  @queryTargetTable = @op_schema_name+'.HC_GENERAL_ROWGROUP_HEALTH'+@tableRunTag
            SET  @queryText =
                N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                    DROP  TABLE '+@queryTargetTable
            EXEC (@queryText)
            ---------------------
            SET  @queryTargetTable = @op_schema_name+'.HC_DISTRIBUTION_LAYOUT'+@tableRunTag
            SET  @queryText =
                N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                    DROP  TABLE '+@queryTargetTable
            EXEC (@queryText)
            ---------------------
            SET  @queryTargetTable = @op_schema_name+'.HC_DISTRIBUTION_SKEW_INFO'+@tableRunTag
            SET  @queryText =
                N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                    DROP  TABLE '+@queryTargetTable
            EXEC (@queryText)
            ---------------------
            SET  @queryTargetTable = @op_schema_name+'.HC_TABLE_STATS_INFO'+@tableRunTag
            SET  @queryText =
                N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                    DROP  TABLE '+@queryTargetTable
            EXEC (@queryText)
            ---------------------
            SET  @queryTargetTable = @op_schema_name+'.HC_BASE'+@tableRunTag
            SET  @queryText =
                N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                    DROP  TABLE '+@queryTargetTable
            EXEC (@queryText)
            ---------------------
            SET  @queryTargetTable = @op_schema_name+'.HC_SCORES_FLAGS'+@tableRunTag
            SET  @queryText =
                N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                    DROP  TABLE '+@queryTargetTable
            EXEC (@queryText)
            ---------------------
            SET  @queryTargetTable = @op_schema_name+'.HC_WITH_IMPORTANCE'+@tableRunTag
            SET  @queryText =
                N'IF OBJECT_ID('''+@queryTargetTable+''') IS NOT NULL
                    DROP  TABLE '+@queryTargetTable
            EXEC (@queryText)
        END 
    END       
END
*/