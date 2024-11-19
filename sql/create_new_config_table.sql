-- Step 1: Create a new table with the updated structure
CREATE
OR REPLACE TABLE `insightsprod.edm_insights_metadata.latency_alerts_params_new` (
    dataset STRING,
    tables ARRAY < STRING >,
    threshold_hours INT64,
    group_by_column STRING,
    last_updated_column STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Step 2: Copy data from the old table to the new table
INSERT INTO
    `insightsprod.edm_insights_metadata.latency_alerts_params_new` (
        dataset,
        tables,
        threshold_hours,
        created_at,
        updated_at
    )
SELECT
    dataset,
    NULL AS tables, -- We'll update this in the next script
    threshold_hours,
    created_at,
    updated_at
FROM
    `insightsprod.edm_insights_metadata.latency_alerts_params`;

-- Step 3: Rename the tables
ALTER TABLE `insightsprod.edm_insights_metadata.latency_alerts_params`
RENAME TO `insightsprod.edm_insights_metadata.latency_alerts_params_old`;

ALTER TABLE `insightsprod.edm_insights_metadata.latency_alerts_params_new`
RENAME TO `insightsprod.edm_insights_metadata.latency_alerts_params`;