-- https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/performance-tuning-materialized-views

-- show the enabled MV's
SELECT V.name as materialized_view, V.object_id, *
FROM sys.views V
JOIN sys.indexes I ON V.object_id= I.object_id AND I.index_id < 2
where is_disabled = 0

-- show the disabled MV's
SELECT V.name as materialized_view, V.object_id, *
FROM sys.views V
JOIN sys.indexes I ON V.object_id= I.object_id AND I.index_id < 2
where is_disabled = 1

-- the DMV's the MV's
select * from sys.pdw_materialized_view_column_distribution_properties
select * from sys.pdw_materialized_view_distribution_properties
select * from sys.pdw_materialized_view_mappings

-- https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-pdw-materialized-view-column-distribution-properties-transact-sql?view=azure-sqldw-latest
-- https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-pdw-materialized-view-distribution-properties-transact-sql?view=azure-sqldw-latest
-- https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-pdw-materialized-view-mappings-transact-sql?view=azure-sqldw-latest

-- Use this to monitor the overheads
DBCC PDW_SHOWMATERIALIZEDVIEWOVERHEAD ("dbo.yourviedwname")
--https://docs.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-pdw-showmaterializedviewoverhead-transact-sql?view=azure-sqldw-latest
