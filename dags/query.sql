WITH
-- ---------------------------------------------------------------------
-- Latency patterns
-- ---------------------------------------------------------------------
patterns AS (
  SELECT *
  FROM `insightsprod.edm_insights_metadata.raw_table_latency_thresholds`
),

-- ---------------------------------------------------------------------
-- TABLE_STORAGE (pre-collected across all projects and regions)
-- ---------------------------------------------------------------------
table_storage_all AS (
  SELECT *
  FROM `insightsprod.edm_insights_metadata._latency_staging_table_storage`
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
      OR table_schema LIKE '%daton%'
      OR UPPER(table_schema) LIKE '%BQ%'
      OR table_schema IN ('nexus_gds_raw')
    )
    AND table_schema NOT IN (
      'daton_healthycell_kr',
      'DatonPearlWestGroup',
      'IH_Daton',
      'daton_eskiin_custom_backup',
      'lhappyinnov_daton_backup',
      'marketdefense_daton_staging',
      'wellbeam_daton_backup'
    )
    AND NOT (source_project_id = 'insightsprod' AND table_schema = 'Manta_Daton')
    AND NOT (source_project_id = 'insightsprod' AND table_schema = 'daton_instanthydrati')
    AND NOT (source_project_id = 'pulse-instanthydration' AND table_schema = 'daton_instanthydrati')
    AND NOT (source_project_id = 'insightsprod' AND table_schema = 'daton_javvycofee')
    AND NOT (source_project_id = 'insightsprod' AND table_schema = 'wellbeam_daton')
    AND table_name != 'daton_metadata'
  GROUP BY table_schema, table_name
),

-- ---------------------------------------------------------------------
-- Dataset labels (pre-collected, ignore latency_check_ignore=true)
-- ---------------------------------------------------------------------
dataset_labels AS (
  SELECT DISTINCT schema_name
  FROM `insightsprod.edm_insights_metadata._latency_staging_dataset_labels`
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
-- Latency evaluation: single-pass pattern matching with daton fallback
-- ---------------------------------------------------------------------
matched_tables AS (
  SELECT
    dts.table_schema,
    dts.table_name,
    DATETIME(dts.storage_last_modified_time, "Asia/Kolkata") AS last_update,
    COALESCE(p.latency_threshold, 24) AS latency_threshold,
    TIMESTAMP_DIFF(
      DATETIME(CURRENT_TIMESTAMP(), "Asia/Kolkata"),
      DATETIME(dts.storage_last_modified_time, "Asia/Kolkata"),
      HOUR
    ) AS hours_since_last_update,
    dts.present_in_projects
  FROM deduped_table_storage dts
  LEFT JOIN patterns p
    ON LOWER(dts.table_name) LIKE p.table_pattern
   AND p.latency_threshold IS NOT NULL
  LEFT JOIN dataset_labels dl
    ON dl.schema_name = dts.table_schema
  LEFT JOIN table_labels tl
    ON tl.table_schema = dts.table_schema
   AND tl.table_name = dts.table_name
  WHERE
    (p.table_pattern IS NOT NULL OR dts.table_schema LIKE '%daton%' OR UPPER(dts.table_schema) LIKE '%BQ%')
    AND dl.schema_name IS NULL
    AND tl.table_name IS NULL
    AND DATETIME(dts.storage_last_modified_time, "Asia/Kolkata")
        < TIMESTAMP_SUB(
            DATETIME(CURRENT_TIMESTAMP(), "Asia/Kolkata"),
            INTERVAL COALESCE(p.latency_threshold, 24) HOUR
          )
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
