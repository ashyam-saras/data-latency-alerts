WITH
-- ---------------------------------------------------------------------
-- Latency patterns
-- ---------------------------------------------------------------------
patterns AS (
  SELECT *
  FROM `insightsprod.edm_insights_metadata.raw_table_latency_thresholds`
),

-- ---------------------------------------------------------------------
-- TABLE_STORAGE across dynamic projects
-- ---------------------------------------------------------------------
table_storage_all AS (
{% for project in var.json.LATENCY_ALERTS__PROJECT_NAME %}
  SELECT
    '{{ project }}' AS source_project_id,
    *
  FROM `{{ project }}.region-us-central1.INFORMATION_SCHEMA.TABLE_STORAGE`
  {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
),

-- ---------------------------------------------------------------------
-- Deduplicate tables across projects
-- ---------------------------------------------------------------------
deduped_table_storage AS (
  SELECT
    table_schema,
    table_name,
    MAX(storage_last_modified_time) AS storage_last_modified_time,
    ARRAY_AGG(DISTINCT source_project_id) AS present_in_projects
  FROM table_storage_all
  WHERE deleted = FALSE
    AND (
      table_schema LIKE '%_prod_raw'
      OR table_schema IN ('nexus_gds_raw')
    )
  GROUP BY table_schema, table_name
),

-- ---------------------------------------------------------------------
-- Dataset labels (ignore latency_check_ignore=true)
-- ---------------------------------------------------------------------
dataset_labels AS (
{% for project in var.json.LATENCY_ALERTS__PROJECT_NAME %}
  SELECT
    '{{ project }}' AS project_id,
    schema_name
  FROM `{{ project }}.region-us-central1.INFORMATION_SCHEMA.SCHEMATA_OPTIONS`
  WHERE option_name = 'labels'
    AND option_value LIKE '%"latency_check_ignore", "true"%'
  {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
),

-- ---------------------------------------------------------------------
-- Explicit table ignore list
-- ---------------------------------------------------------------------
table_labels AS (
  SELECT DISTINCT
    table_schema,
    table_name
  FROM `insightsprod.edm_insights_metadata.ignore_latency_tables_list`
),

-- ---------------------------------------------------------------------
-- Latency evaluation
-- ---------------------------------------------------------------------
matched_tables AS (
  SELECT
    dts.table_schema,
    dts.table_name,
    DATETIME(dts.storage_last_modified_time, "Asia/Kolkata") AS last_update,
    p.latency_threshold,
    TIMESTAMP_DIFF(
      DATETIME(CURRENT_TIMESTAMP(), "Asia/Kolkata"),
      DATETIME(dts.storage_last_modified_time, "Asia/Kolkata"),
      HOUR
    ) AS hours_since_last_update,
    dts.present_in_projects
  FROM deduped_table_storage dts
  CROSS JOIN patterns p
  LEFT JOIN dataset_labels dl
    ON dl.schema_name = dts.table_schema
  LEFT JOIN table_labels tl
    ON tl.table_schema = dts.table_schema
   AND tl.table_name = dts.table_name
  WHERE
    LOWER(dts.table_name) LIKE p.table_pattern
    AND p.latency_threshold IS NOT NULL
    AND DATETIME(dts.storage_last_modified_time, "Asia/Kolkata")
        < TIMESTAMP_SUB(
            DATETIME(CURRENT_TIMESTAMP(), "Asia/Kolkata"),
            INTERVAL p.latency_threshold HOUR
          )
    AND dl.schema_name IS NULL
    AND tl.table_name IS NULL
),

-- ---------------------------------------------------------------------
-- Deduplicate matched tables by table_schema and table_name
-- Keep the row with maximum last_update if table exists in multiple projects
-- ---------------------------------------------------------------------
deduped_matched_tables AS (
  SELECT
    table_schema,
    table_name,
    last_update,
    latency_threshold,
    hours_since_last_update,
    present_in_projects
  FROM matched_tables
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY table_schema, table_name
      ORDER BY last_update DESC
    ) = 1
),

-- ---------------------------------------------------------------------
-- Active source tables
-- ---------------------------------------------------------------------
source_tables AS (
  SELECT
    tableid,
    sourceid,
    companyid,
    status,
    tablename
  FROM `insightsprod.pulse_metadata.gcp_postgres_daton_public_source_tables`
  WHERE selected = TRUE
    AND status = 2
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY sourceid, tableid
      ORDER BY _daton_batch_runtime DESC
    ) = 1
),

-- ---------------------------------------------------------------------
-- Source metadata
-- ---------------------------------------------------------------------
sources AS (
  SELECT
    sourceid,
    companyid,
    type,
    CASE
      WHEN status = 2 THEN 'Active'
      WHEN status = 11 THEN 'Rescheduled'
      WHEN status = 6 THEN 'Paused'
      WHEN status = 5 THEN 'Deleted'
      ELSE 'Unknown'
    END AS status,
    name
  FROM `insightsprod.pulse_metadata.gcp_postgres_daton_public_source`
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY sourceid
      ORDER BY _daton_batch_runtime DESC
    ) = 1
),

-- ---------------------------------------------------------------------
-- Last job stats
-- ---------------------------------------------------------------------
last_job_stats AS (
  SELECT
    tableid,
    sourceid,
    lasterrorcode,
    lasterror,
    DATETIME(
      TIMESTAMP_MILLIS(CAST(lastendtime AS INT64)),
      "Asia/Kolkata"
    ) AS last_job_end_time
  FROM `insightsprod.pulse_metadata.gcp_postgres_daton_public_last_job_stats`
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY sourceid, tablename
      ORDER BY _daton_batch_runtime DESC
    ) = 1
),

-- ---------------------------------------------------------------------
-- Final enrichment
-- ---------------------------------------------------------------------
final AS (
  SELECT
    st.companyid,
    s.type AS platform,
    s.status AS source_status,
    ljs.lasterrorcode,
    ljs.lasterror,
    ljs.last_job_end_time,
    s.name || '_' || st.tablename AS table_name
  FROM source_tables st
  LEFT JOIN sources s USING (sourceid)
  LEFT JOIN last_job_stats ljs USING (tableid)
)

-- ---------------------------------------------------------------------
-- Final output
-- ---------------------------------------------------------------------
SELECT
  mt.table_schema,
  mt.table_name,
  FORMAT_DATETIME('%F %H:%M:%S', mt.last_update) AS last_update,
  mt.latency_threshold,
  mt.hours_since_last_update,
  mt.present_in_projects,
  f.platform,
  f.source_status,
  f.lasterrorcode,
  f.lasterror,
  FORMAT_DATETIME('%F %H:%M:%S', f.last_job_end_time) AS last_job_end_time
FROM deduped_matched_tables mt
LEFT JOIN final f
  ON mt.table_name = f.table_name
ORDER BY hours_since_last_update DESC;
