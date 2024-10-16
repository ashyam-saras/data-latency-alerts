WITH config AS (
  SELECT 
    dataset,
    table_name,
    threshold_hours,
    last_updated_column,
    inclusion_rule
  FROM 
    `{project_name}.{audit_dataset_name}.{latency_params_table}`,
    UNNEST(tables) AS table_name
  WHERE dataset = '{dataset_id}'
    AND group_by_column IS NULL
),
dataset_config AS (
  SELECT 
    dataset,
    ANY_VALUE(threshold_hours) AS threshold_hours
  FROM config
  GROUP BY dataset
)
SELECT 
  '{project_name}' AS project_id,
  t.dataset_id,
  t.table_id,
  COALESCE(c.threshold_hours, dc.threshold_hours) AS threshold_hours,
  COALESCE(c.last_updated_column, 'last_modified_time') AS last_updated_column,
  TIMESTAMP_MILLIS(t.last_modified_time) AS last_modified_time,
  TIMESTAMP_DIFF(
    CURRENT_TIMESTAMP(),
    TIMESTAMP_MILLIS(t.last_modified_time),
    HOUR
  ) AS hours_since_update
FROM 
  `{project_name}.{dataset_id}.__TABLES__` t
LEFT JOIN 
  config c
ON 
  t.table_id = c.table_name
CROSS JOIN
  dataset_config dc
WHERE 
  t.type = 1
  AND (c.inclusion_rule IS NULL OR c.inclusion_rule = 'INCLUDE')
  AND TIMESTAMP_DIFF(
    CURRENT_TIMESTAMP(),
    TIMESTAMP_MILLIS(t.last_modified_time),
    HOUR
  ) >= COALESCE(c.threshold_hours, dc.threshold_hours)
ORDER BY 
  hours_since_update DESC
