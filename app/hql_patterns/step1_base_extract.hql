SELECT DISTINCT
    ${required_columns}
FROM ${source_table}
WHERE partition_batch_ts >= ${partition_batch_ts}
AND SUBSTRING(${date_col}, 1, 10) >= '${start_date}'
AND banner = '${banner}'
AND record_type = '02'