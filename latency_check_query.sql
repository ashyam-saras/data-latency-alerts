WITH config AS (
  SELECT 
    dataset,
    table_name,
    inclusion_rule,
    threshold_hours,
    group_by_column,
    last_updated_column
  FROM 
    `{project_name}.{audit_dataset_name}.{latency_params_table}`,
    UNNEST(tables) AS table_name
  WHERE dataset = '{dataset_id}'
)
SELECT 
  t.project_id,
  t.dataset_id,
  t.table_id,
  COALESCE(c.threshold_hours, 24) AS threshold_hours,
  COALESCE(c.inclusion_rule, 'INCLUDE') AS inclusion_rule,
  c.group_by_column,
  COALESCE(c.last_updated_column, 'last_modified_time') AS last_updated_column,
  CASE
    WHEN c.group_by_column IS NOT NULL THEN
      (SELECT AS STRUCT
        MAX(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', CAST(data[OFFSET(0)] AS STRING))) AS last_modified_time,
        DATETIME_DIFF(CURRENT_DATETIME('Asia/Calcutta'), MAX(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', CAST(data[OFFSET(0)] AS STRING))), HOUR) AS hours_since_update,
        STRING(data[OFFSET(1)]) AS group_by_value
       FROM (
         SELECT 
           ARRAY_AGG(STRUCT(
             CAST(data[c.last_updated_column] AS STRING),
             CAST(data[c.group_by_column] AS STRING)
           )) AS data
         FROM `{project_name}.{dataset_id}.{table_id}` data
         GROUP BY data[c.group_by_column]
       )
      )
    ELSE
      STRUCT(
        DATETIME(TIMESTAMP_MILLIS(t.last_modified_time), "Asia/Calcutta") AS last_modified_time,
        DATETIME_DIFF(CURRENT_DATETIME('Asia/Calcutta'), DATETIME(TIMESTAMP_MILLIS(t.last_modified_time), "Asia/Calcutta"), HOUR) AS hours_since_update,
        NULL AS group_by_value
      )
  END AS update_info
FROM 
  `{project_name}.{dataset_id}.__TABLES__` t
LEFT JOIN 
  config c
ON 
  t.table_id = c.table_name
WHERE 
  t.type = 'TABLE'
  AND (c.inclusion_rule = 'INCLUDE' OR c.inclusion_rule IS NULL)
  AND (c.table_name IS NULL OR t.table_id = c.table_name)  -- This line handles both individual and set of tables
ORDER BY 
  update_info.hours_since_update DESC
