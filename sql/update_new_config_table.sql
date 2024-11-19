-- Step 1: Update the 'nexusbrand_3642_prod_raw' dataset with included tables
WITH
    ExcludeTables AS (
        SELECT
            dataset,
            SPLIT (exclude_list, ',') AS exclude_tables
        FROM
            `insightsprod.edm_insights_metadata.latency_alerts_params_old`
        WHERE
            dataset = 'nexusbrand_3642_prod_raw'
    ),
    IncludedTables AS (
        SELECT
            'nexusbrand_3642_prod_raw' AS dataset,
            ARRAY_AGG (t.table_id) AS included_tables
        FROM
            `insightsprod.nexusbrand_3642_prod_raw.__TABLES__` t
            LEFT JOIN ExcludeTables e ON t.table_id IN UNNEST (e.exclude_tables)
        WHERE
            e.exclude_tables IS NULL
    )
UPDATE `insightsprod.edm_insights_metadata.latency_alerts_params` p
SET
    p.tables = i.included_tables
FROM
    IncludedTables i
WHERE
    p.dataset = i.dataset;

-- Step 2: Verify the update
SELECT
    *
FROM
    `insightsprod.edm_insights_metadata.latency_alerts_params`
WHERE
    dataset = 'nexusbrand_3642_prod_raw';

-- Step 3: Clean up (optional)
-- Only run this after verifying that the migration was successful
-- DROP TABLE `insightsprod.edm_insights_metadata.latency_alerts_params_old`;