WITH config AS (
  SELECT 
    dataset,
    threshold_hours,
    last_updated_column
  FROM 
    `{project_name}.{audit_dataset_name}.{latency_params_table}`
  WHERE dataset = '{dataset_id}'
    AND group_by_column IS NULL
)
SELECT 
  '{project_name}' AS project_id,
  t.dataset_id,
  t.table_id,
  c.threshold_hours,
  c.last_updated_column AS last_updated_column,
  TIMESTAMP_MILLIS(t.last_modified_time) AS last_modified_time,
  TIMESTAMP_DIFF(
    CURRENT_TIMESTAMP(),
    TIMESTAMP_MILLIS(t.last_modified_time),
    HOUR
  ) AS hours_since_update
FROM 
  `{project_name}.{dataset_id}.__TABLES__` t
CROSS JOIN 
  config c
WHERE 
  t.type = 1
  AND TIMESTAMP_DIFF(
    CURRENT_TIMESTAMP(),
    TIMESTAMP_MILLIS(t.last_modified_time),
    HOUR
  ) >= c.threshold_hours
ORDER BY 
  hours_since_update DESC
