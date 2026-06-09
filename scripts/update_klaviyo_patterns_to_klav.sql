-- Update Klaviyo latency patterns: %klaviyo% -> %klav%
-- Covers KLAV3, KLAVIYO3, and legacy klaviyo table naming in *_prod_raw.
-- Target: insightsprod.edm_insights_metadata.raw_table_latency_thresholds

-- ---------------------------------------------------------------------------
-- 1) Preview rows that will change
-- ---------------------------------------------------------------------------
SELECT
  table_pattern AS current_pattern,
  REPLACE(table_pattern, '%klaviyo%', '%klav%') AS new_pattern,
  latency_threshold
FROM `insightsprod.edm_insights_metadata.raw_table_latency_thresholds`
WHERE LOWER(table_pattern) LIKE '%klaviyo%'
ORDER BY table_pattern;

-- ---------------------------------------------------------------------------
-- 2) Apply update (15 rows as of 2026-05-19)
-- ---------------------------------------------------------------------------
UPDATE `insightsprod.edm_insights_metadata.raw_table_latency_thresholds`
SET table_pattern = REPLACE(table_pattern, '%klaviyo%', '%klav%')
WHERE LOWER(table_pattern) LIKE '%klaviyo%'
  AND latency_threshold = 24;

-- ---------------------------------------------------------------------------
-- 3) Verify after update
-- ---------------------------------------------------------------------------
SELECT table_pattern, latency_threshold
FROM `insightsprod.edm_insights_metadata.raw_table_latency_thresholds`
WHERE LOWER(table_pattern) LIKE '%klav%'
ORDER BY table_pattern;

-- ---------------------------------------------------------------------------
-- 4) Spot-check: PAVOI KLAV3 campaigns should now match
-- ---------------------------------------------------------------------------
SELECT
  'PAVOI_KLAV3_3114_campaigns' AS table_name,
  LOWER('PAVOI_KLAV3_3114_campaigns') LIKE '%klav%campaigns' AS matches_new_campaigns_pattern;
