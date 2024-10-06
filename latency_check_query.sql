WITH dataset_params AS (
  SELECT dp.dataset, dp.exclude_list, dp.threshhold_hours
  FROM `{project_name}.{audit_dataset_name}.{latency_params_table}` dp
  JOIN `{project_name}.INFORMATION_SCHEMA.SCHEMATA` s
    ON dp.dataset = s.schema_name
),
table_info AS (
  SELECT
    t.project_id,
    t.dataset_id,
    t.table_id,
    DATETIME(TIMESTAMP_MILLIS(t.last_modified_time), "Asia/Calcutta") AS last_modified_at_IST,
    CURRENT_DATETIME('Asia/Calcutta') AS current_time_IST,
    DATETIME_DIFF(CURRENT_DATETIME('Asia/Calcutta'), DATETIME(TIMESTAMP_MILLIS(t.last_modified_time), "Asia/Calcutta"), HOUR) AS hours_since_update,
    dp.threshhold_hours,
    dp.exclude_list
  FROM dataset_params dp
  JOIN `{project_name}.{dataset_id}.__TABLES__` t ON dp.dataset = t.dataset_id
)
SELECT DISTINCT
  project_id,
  dataset_id,
  table_id,
  last_modified_at_IST,
  current_time_IST,
  hours_since_update,
  threshhold_hours
FROM table_info
WHERE hours_since_update > threshhold_hours
  AND (exclude_list IS NULL OR NOT REGEXP_CONTAINS(table_id, CONCAT(r'(', REPLACE(exclude_list, ',', '|'), r')')))
ORDER BY hours_since_update DESC;