--https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-overview
SELECT
    [Fully Entity Name]                 = t.full_entity_name,
    [Schema Name]                       = t.schema_name,
    [Entity Name]                       = t.entity_name,
    [Current Distribution Method]       = t.distribution_method,
    [Current Distribution Column]       = ISNULL(t.distribution_column, '-'),
    [Current Rows]                      = SUM(t.rows_count),
    [Distribution Count]                = COUNT(t.rows_count),
    [Current Data Size on Disk MB]      = SUM(t.data_size_MB),
	[Current Data Size on Disk GB]      = SUM(t.data_size_MB)/1024,
    [Current Index Size on Disk MB]     = SUM(t.index_size_MB),
    [Skew Coefficient]                  = CASE
                                            WHEN SUM(t.rows_count) / COUNT(rows_count) <> 0
                                            THEN ROUND((t.standard_deviation / (SUM(t.rows_count) / COUNT(t.rows_count))) * 1.0, 2)
                                            ELSE 0
                                          END,
    [Skew Percentage]                   = CASE
                                            WHEN MAX(t.rows_count) <> 0
                                            THEN CAST((100.0 - (AVG(CAST(t.rows_count as float)) / MAX(t.rows_count) * 100)) AS DECIMAL(4, 2))
                                            ELSE 0
                                          END
FROM
(
    SELECT
        full_entity_name        = QUOTENAME(s.name) + '.' + QUOTENAME(t.name),
        schema_name             = s.name,
        entity_name             = t.name,
        distribution_method     = tp.distribution_policy_desc,
        distribution_column     = c.name,
        rows_count              = nps.row_count,
        data_size_MB            = (
                                    (
                                        nps.in_row_data_page_count +
                                        nps.row_overflow_used_page_count +
                                        nps.lob_used_page_count
                                    ) * 8.0
                                  ) / 1000,
        index_size_MB           = (
                                    (
                                        nps.reserved_page_count - (nps.reserved_page_count - nps.used_page_count) -
                                        (nps.in_row_data_page_count + nps.row_overflow_used_page_count + nps.lob_used_page_count)
                                    ) * 8.0
                                  ) / 1000,
        standard_deviation      = STDEV(nps.row_count) OVER (PARTITION BY t.object_id)
    FROM
        sys.schemas AS s
        INNER JOIN sys.tables AS t
            ON s.schema_id = t.schema_id
        INNER JOIN sys.indexes AS i
            ON t.object_id = i.object_id
            AND i.index_id <= 1
        INNER JOIN sys.pdw_table_distribution_properties AS tp
            ON t.object_id = tp.object_id
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
        LEFT JOIN sys.columns AS c
            ON cdp.object_id = c.object_id
            AND cdp.column_id = c.column_id
    WHERE
        pn.type = 'COMPUTE'
) AS t
GROUP BY
    t.full_entity_name,
    t.schema_name,
    t.entity_name,
    t.distribution_method,
    t.distribution_column,
    t.standard_deviation;