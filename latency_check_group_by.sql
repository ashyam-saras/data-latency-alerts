WITH config AS (
  SELECT 
    dataset,
    table_name,
    threshold_hours,
    group_by_column,
    last_updated_column
  FROM 
    `{project_name}.{audit_dataset_name}.{latency_params_table}`,
    UNNEST(tables) AS table_name
  WHERE dataset = '{dataset_id}'
    AND group_by_column IS NOT NULL
)
SELECT 
  '{project_name}' AS project_id,
  c.dataset AS dataset_id,
  c.table_name AS table_id,
  c.threshold_hours,
  c.group_by_column,
  c.last_updated_column,
  MAX(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S%Ez', CAST({last_updated_column} AS STRING))) AS last_modified_time,
  TIMESTAMP_DIFF(
    CURRENT_TIMESTAMP(),
    MAX(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S%Ez', CAST({last_updated_column} AS STRING))),
    HOUR
  ) AS hours_since_update,
  {group_by_column} AS group_by_value
FROM 
  `{project_name}.{dataset_id}.{table_id}` data
JOIN
  config c
ON
  c.table_name = '{table_id}'
GROUP BY 
  c.dataset,
  c.table_name,
  c.threshold_hours,
  c.group_by_column,
  c.last_updated_column,
  {group_by_column}
HAVING
  hours_since_update > c.threshold_hours
ORDER BY 
  hours_since_update DESC
