-- Create the LATENCY_PARAMS_TABLE with ARRAY of table names and metadata columns
CREATE OR REPLACE TABLE `insightsprod.edm_insights_metadata.latency_alerts_params_dev` (
  dataset STRING,
  tables ARRAY<STRING>,
  threshold_hours INT64,
  group_by_column STRING,
  last_updated_column STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Insert test scenarios (now including created_at and updated_at)

-- Scenario 1: Dataset-level configuration (no group by, no last updated column)
INSERT INTO `insightsprod.edm_insights_metadata.latency_alerts_params_dev`
(dataset, tables, threshold_hours, created_at, updated_at)
VALUES ('test_dataset1', NULL, 24, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Scenario 2: Table-level configuration (no group by, no last updated column)
INSERT INTO `insightsprod.edm_insights_metadata.latency_alerts_params_dev`
(dataset, tables, threshold_hours, created_at, updated_at)
VALUES (
  'test_dataset2',
  ['test_table1', 'test_table2'],
  12,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);

-- Scenario 3: Group-by configuration (both group by and last updated column)
INSERT INTO `insightsprod.edm_insights_metadata.latency_alerts_params_dev`
(dataset, tables, threshold_hours, group_by_column, last_updated_column, created_at, updated_at)
VALUES (
  'test_dataset3',
  ['test_table3'],
  6,
  'brand',
  'last_updated_at',
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
);

-- Scenario 4: Mixed configuration (some tables with group by, some without)
INSERT INTO `insightsprod.edm_insights_metadata.latency_alerts_params_dev`
(dataset, tables, threshold_hours, group_by_column, last_updated_column, created_at, updated_at)
VALUES 
  ('test_dataset4', ['test_table4a', 'test_table4b'], 18, 'category', 'update_time', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Scenario 5: Multiple tables in a dataset
INSERT INTO `insightsprod.edm_insights_metadata.latency_alerts_params_dev`
(dataset, tables, threshold_hours, created_at, updated_at)
VALUES 
  ('test_dataset5', ['test_table5a', 'test_table5b', 'test_table5c'], 48, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Scenario 6: Dataset with multiple tables, different thresholds
INSERT INTO `insightsprod.edm_insights_metadata.latency_alerts_params_dev`
(dataset, tables, threshold_hours, created_at, updated_at)
VALUES 
  ('test_dataset6', ['test_table6a'], 8, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
  ('test_dataset6', ['test_table6b'], 16, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
  ('test_dataset6', ['test_table6c'], 24, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Create test datasets and tables

-- For Scenario 1
CREATE SCHEMA IF NOT EXISTS `insightsprod.test_dataset1`;
CREATE OR REPLACE TABLE `insightsprod.test_dataset1.test_table1` (
  id INT64,
  data STRING,
  last_modified_time TIMESTAMP
);
CREATE OR REPLACE TABLE `insightsprod.test_dataset1.test_table2` (
  id INT64,
  data STRING,
  last_modified_time TIMESTAMP
);

-- For Scenario 2
CREATE SCHEMA IF NOT EXISTS `insightsprod.test_dataset2`;
CREATE OR REPLACE TABLE `insightsprod.test_dataset2.test_table1` (
  id INT64,
  data STRING,
  last_modified_time TIMESTAMP
);
CREATE OR REPLACE TABLE `insightsprod.test_dataset2.test_table2` (
  id INT64,
  data STRING,
  last_modified_time TIMESTAMP
);

-- For Scenario 3
CREATE SCHEMA IF NOT EXISTS `insightsprod.test_dataset3`;
CREATE OR REPLACE TABLE `insightsprod.test_dataset3.test_table3` (
  id INT64,
  brand STRING,
  data STRING,
  last_updated_at TIMESTAMP
);

-- For Scenario 4
CREATE SCHEMA IF NOT EXISTS `insightsprod.test_dataset4`;
CREATE OR REPLACE TABLE `insightsprod.test_dataset4.test_table4a` (
  id INT64,
  category STRING,
  data STRING,
  update_time TIMESTAMP
);
CREATE OR REPLACE TABLE `insightsprod.test_dataset4.test_table4b` (
  id INT64,
  category STRING,
  data STRING,
  update_time TIMESTAMP
);

-- For Scenario 5
CREATE SCHEMA IF NOT EXISTS `insightsprod.test_dataset5`;
CREATE OR REPLACE TABLE `insightsprod.test_dataset5.test_table5a` (
  id INT64,
  data STRING,
  last_modified_time TIMESTAMP
);
CREATE OR REPLACE TABLE `insightsprod.test_dataset5.test_table5b` (
  id INT64,
  data STRING,
  last_modified_time TIMESTAMP
);
CREATE OR REPLACE TABLE `insightsprod.test_dataset5.test_table5c` (
  id INT64,
  data STRING,
  last_modified_time TIMESTAMP
);

-- For Scenario 6
CREATE SCHEMA IF NOT EXISTS `insightsprod.test_dataset6`;
CREATE OR REPLACE TABLE `insightsprod.test_dataset6.test_table6a` (
  id INT64,
  data STRING,
  last_modified_time TIMESTAMP
);
CREATE OR REPLACE TABLE `insightsprod.test_dataset6.test_table6b` (
  id INT64,
  data STRING,
  last_modified_time TIMESTAMP
);
CREATE OR REPLACE TABLE `insightsprod.test_dataset6.test_table6c` (
  id INT64,
  data STRING,
  last_modified_time TIMESTAMP
);

-- Insert test data

-- Scenario 1: Data within and outside threshold for dataset-level config
INSERT INTO `insightsprod.test_dataset1.test_table1` (id, data, last_modified_time)
VALUES (1, 'Recent data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR));
INSERT INTO `insightsprod.test_dataset1.test_table2` (id, data, last_modified_time)
VALUES (1, 'Old data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 36 HOUR));

-- Scenario 2: Data for table-level config
INSERT INTO `insightsprod.test_dataset2.test_table1` (id, data, last_modified_time)
VALUES 
  (1, 'Recent data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)),
  (2, 'Old data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR));

INSERT INTO `insightsprod.test_dataset2.test_table2` (id, data, last_modified_time)
VALUES 
  (1, 'Recent data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)),
  (2, 'Old data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 18 HOUR));

-- Scenario 3: Data for group-by config
INSERT INTO `insightsprod.test_dataset3.test_table3` (id, brand, data, last_updated_at)
VALUES 
  (1, 'BrandA', 'Recent data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)),
  (2, 'BrandB', 'Old data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR)),
  (3, 'BrandC', 'Very old data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 72 HOUR));

-- Scenario 4: Mixed configuration data
INSERT INTO `insightsprod.test_dataset4.test_table4a` (id, category, data, update_time)
VALUES
  (1, 'CategoryA', 'Recent data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)),
  (2, 'CategoryB', 'Old data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR));

INSERT INTO `insightsprod.test_dataset4.test_table4b` (id, category, data, update_time)
VALUES
  (1, 'CategoryC', 'Recent data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR)),
  (2, 'CategoryD', 'Old data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 36 HOUR));

-- Scenario 5: Multiple tables in a dataset
INSERT INTO `insightsprod.test_dataset5.test_table5a` (id, data, last_modified_time)
VALUES (1, 'Recent data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR));

INSERT INTO `insightsprod.test_dataset5.test_table5b` (id, data, last_modified_time)
VALUES (1, 'Old data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 72 HOUR));

INSERT INTO `insightsprod.test_dataset5.test_table5c` (id, data, last_modified_time)
VALUES (1, 'Very old data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 120 HOUR));

-- Scenario 6: Dataset with multiple tables, different thresholds
INSERT INTO `insightsprod.test_dataset6.test_table6a` (id, data, last_modified_time)
VALUES (1, 'Recent data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 HOUR));

INSERT INTO `insightsprod.test_dataset6.test_table6b` (id, data, last_modified_time)
VALUES (1, 'Moderately old data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR));

INSERT INTO `insightsprod.test_dataset6.test_table6c` (id, data, last_modified_time)
VALUES (1, 'Old data', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 HOUR));
