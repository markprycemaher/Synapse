--https://github.com/Microsoft/sql-data-warehouse-samples/blob/main/solutions/monitoring/scripts/views/microsoft.vw_table_sizes.sql
-- Looking at LineItem a "round_robin" distribute table

-- max: 100,025,062
-- min:  99,980,262
-- difference : 44,800


SELECT
    [Entity Name]                   = QUOTENAME(s.name) + '.' + QUOTENAME(t.name),
    [Current Distribution Method]   = tp.distribution_policy_desc,
    [Current Distribution Column]   = c.name,
    [Distribution Name]             = di.name,
    [Row Count]                     = nps.row_count
from
    sys.schemas AS s
    INNER JOIN sys.tables AS t
        ON s.schema_id = t.schema_id
    INNER JOIN sys.indexes AS i
        ON t.object_id = i.object_id
        AND i.index_id <= 1
    INNER JOIN sys.pdw_table_distribution_properties AS tp
        ON  t.object_id = tp.object_id
    INNER JOIN sys.pdw_table_mappings AS tm
        ON t.object_id = tm.object_id
    INNER JOIN sys.pdw_nodes_tables AS nt
        ON tm.physical_name = nt.name
    INNER JOIN sys.dm_pdw_nodes AS pn
        ON nt.pdw_node_id = pn.pdw_node_id
    INNER JOIN sys.pdw_distributions AS di
        ON nt.distribution_id = di.distribution_id
    INNER JOIN
    (
        SELECT
            object_id                       = object_id,
            pdw_node_id                     = pdw_node_id,
            distribution_id                 = distribution_id,
            row_count                       = SUM(row_count),
            in_row_data_page_count          = SUM(in_row_data_page_count),
            row_overflow_used_page_count    = SUM(row_overflow_used_page_count),
            lob_used_page_count             = SUM(lob_used_page_count),
            reserved_page_count             = SUM(reserved_page_count),
            used_page_count                 = SUM(used_page_count)
        FROM
            sys.dm_pdw_nodes_db_partition_stats
        GROUP BY
            object_id,
            pdw_node_id,
            distribution_id
    ) AS nps
        ON nt.object_id = nps.object_id
        AND nt.pdw_node_id = nps.pdw_node_id
        AND nt.distribution_id = nps.distribution_id
    LEFT JOIN
    (
        SELECT
            object_id,
            column_id
        FROM
            sys.pdw_column_distribution_properties
        WHERE
            distribution_ordinal = 1
    ) AS cdp
        ON t.object_id = cdp.object_id
    LEFT JOIN sys.columns as c with(nolock)
        ON cdp.object_id = c.object_id
        AND cdp.column_id = c.column_id
WHERE  QUOTENAME(s.name) + '.' + QUOTENAME(t.name)  
-- = '[dbo].[round_robin_test1]' 
= '[dbo].[lineitem]'
and
    pn.type = 'COMPUTE'
	order by nps.row_count desc;


/*
Max : 100025062
Min : 99980262
*/
