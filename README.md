[![Deploy Cloud Function](https://github.com/ashyam-saras/data-latency-alerts/actions/workflows/deploy-cloud-function.yml/badge.svg)](https://github.com/ashyam-saras/data-latency-alerts/actions/workflows/deploy-cloud-function.yml)
[![Deploy Cloud Scheduler](https://github.com/ashyam-saras/data-latency-alerts/actions/workflows/deploy-cloud-scheduler.yml/badge.svg)](https://github.com/ashyam-saras/data-latency-alerts/actions/workflows/deploy-cloud-scheduler.yml)
![Coverage](reports/coverage/badge.svg)

# Data Latency Alerts

This project provides an automated system to monitor and alert about BigQuery raw tables that have not been updated within specified timeframes. It uses a pattern-based approach to monitor `*_prod_raw` datasets and helps data teams maintain data freshness and quickly identify potential issues in data pipelines.

## Table of Contents
- [Features](#features)
- [Prerequisites](#prerequisites)
- [How it works](#how-it-works)
- [Setup and Configuration](#setup-and-configuration)
  - [Environment Variables](#environment-variables)
  - [BigQuery Setup](#bigquery-setup)
  - [Slack Setup](#slack-setup)
- [Deployment](#deployment)
  - [Development Deployment](#development-deployment)
  - [Production Deployment](#production-deployment)
  - [Environment Variables](#environment-variables-1)
  - [Secrets](#secrets)
- [Orchestration with Cloud Composer](#orchestration-with-cloud-composer)
- [Project Structure](#project-structure)
- [Configuration Management](#configuration-management)
- [Contributing](#contributing)

## Features

- **Pattern-based monitoring** of BigQuery raw tables (`*_prod_raw` datasets)
- **Flexible configuration** using table patterns and latency thresholds
- **Smart exclusions** via dataset labels and table ignore lists
- **Automated alerts** via Slack with summary statistics and detailed Excel reports
- **Cloud Function deployment** with scheduled execution via Cloud Scheduler
- **Automated CI/CD pipeline** using GitHub Actions
- **Comprehensive error handling** and logging

## Prerequisites

Before setting up this project, ensure you have the following:

1. A Google Cloud Platform (GCP) project with billing enabled
2. A service account with the following roles:
   - Cloud Functions Developer
   - Cloud Run Admin
   - Cloud Scheduler Admin
   - Service Account User
   - BigQuery Job User
3. Python 3.11 or later
4. A Slack workspace with permissions to create apps and send messages

## How it works

The system uses a **pattern-based approach** to monitor raw data tables:

### 1. Pattern Matching & Configuration
- Reads table patterns and latency thresholds from `raw_table_latency_thresholds` table
- Matches table names against patterns (e.g., `%amazonsbads_%portfolio` matches `amazonsbads_12345_portfolio`)
- Each pattern has an associated latency threshold (e.g., 24 hours)

### 2. Smart Filtering
- **Dataset-level filtering**: Excludes datasets labeled with `latency_check_ignore=true`
- **Table-level filtering**: Excludes specific tables listed in `ignore_latency_tables_list`
- **Scope**: Only monitors `*_prod_raw` datasets

### 3. Latency Detection
- Uses `INFORMATION_SCHEMA.TABLE_STORAGE` to get accurate table modification times
- Identifies tables where `STORAGE_LAST_MODIFIED_TIME` exceeds the pattern's threshold
- Calculates hours since last update for each violating table

### 4. Alert Generation
- Generates summary statistics (total violations, max delay, average delay)
- Creates Excel reports with detailed violation data
- Sends structured Slack notifications with top 5 datasets by average delay

### 5. Scheduling & Execution
- Triggered via Cloud Scheduler (default: every 6 hours)
- Supports manual execution and dataset-specific filtering
- Comprehensive error handling and reporting

## Setup and Configuration

### Environment Variables

The following environment variables are required:

- `PROJECT_NAME`: GCP project name
- `AUDIT_DATASET_NAME`: Name of the audit dataset containing configuration tables
- `SLACK_CHANNEL_ID`: ID of the Slack channel for alerts
- `SLACK_API_TOKEN`: Slack API token for sending messages

### BigQuery Setup

1. **Create audit dataset**: Create the `AUDIT_DATASET_NAME` dataset in your BigQuery project.

2. **Set up configuration tables**:
   - `raw_table_latency_thresholds`: Contains patterns and thresholds
     ```sql
     CREATE TABLE `{project}.{audit_dataset}.raw_table_latency_thresholds` (
       source STRING,
       table STRING,
       table_pattern STRING,
       latency_threshold INT64
     );
     ```
   - `ignore_latency_tables_list`: Contains tables to exclude from monitoring
     ```sql
     CREATE TABLE `{project}.{audit_dataset}.ignore_latency_tables_list` (
       table_schema STRING,
       table_name STRING
     );
     ```

3. **Configure dataset exclusions** (optional):
   ```sql
   ALTER SCHEMA `{project}.{dataset_name}` 
   SET OPTIONS (labels = [('latency_check_ignore', 'true')]);
   ```

### Slack Setup

1. Create a Slack app in your workspace
2. Get the Bot User OAuth Token and set it as `SLACK_API_TOKEN`
3. Invite the bot to your desired channel
4. Get the channel ID and set it as `SLACK_CHANNEL_ID`

## Deployment

This project uses GitHub Actions for automated deployments:

### Development Deployment
- **Trigger**: Push to `dev` branch or manual dispatch
- **Function name**: `data-latency-alerts-dev`
- **Environment**: Development

### Production Deployment
- **Trigger**: Push to `main` branch or manual dispatch  
- **Function name**: `data-latency-alerts`
- **Environment**: Production

### Secrets
Set these secrets in your GitHub repository:
- `GCP_SA_KEY`: JSON key of the Google Cloud service account
- `SLACK_API_TOKEN`: Slack Bot User OAuth Token

## Orchestration with Cloud Composer

For production workloads, you can orchestrate the data latency alerts using Cloud Composer (Apache Airflow) instead of Cloud Scheduler.

### Benefits of Using Composer
- **Better Monitoring**: Rich UI for tracking DAG runs and task status
- **Retry Logic**: Advanced retry and failure handling capabilities
- **Workflow Management**: Complex dependencies and conditional execution
- **Logging**: Centralized logging and debugging capabilities
- **Manual Triggers**: Easy manual execution and dataset-specific runs

### DAG Deployment

The project includes Airflow DAGs that can be deployed to your existing Cloud Composer environment:

1. **Automatic Deployment**: Push changes to `main` branch or files in `dags/` folder
2. **Manual Deployment**: Use GitHub Actions workflow dispatch
3. **Configure Repository Variables**:
   - `COMPOSER_ENVIRONMENT_NAME`: Your Composer environment name
   - `COMPOSER_LOCATION`: Environment location (default: us-central1)

### DAG Schedule
- **Main DAG**: `data_latency_alerts` - Runs twice daily at 6 AM and 6 PM IST
- **Dataset-specific DAG**: `data_latency_alerts_dataset_specific` - Manual trigger for ad-hoc checks

### Required Airflow Variables
Set these in your Composer environment:
- `DATA_LATENCY_CLOUD_FUNCTION_URL`
- `DATA_LATENCY_SLACK_CHANNEL_ID`
- `DATA_LATENCY_SLACK_API_TOKEN`
- `DATA_LATENCY_PROJECT_NAME`

## Project Structure

```
data-latency-alerts/
├── main.py                                 # Cloud Function entry point
├── requirements.txt                        # Python dependencies
├── utils/                                  # Utility modules
│   ├── bigquery.py                        # BigQuery operations
│   ├── slack.py                           # Slack notifications
│   ├── log.py                             # Logging utilities
│   └── utils.py                           # Helper functions
├── sql/                                   # SQL queries
│   └── pattern_based_latency_check.sql   # Main monitoring query
├── dags/                                  # Airflow DAGs
│   └── data_latency_alerts_dag.py        # Cloud Composer orchestration
├── tests/                                 # Test files
└── .github/workflows/                     # CI/CD workflows
    ├── deploy-cloud-function.yml         # Cloud Function deployment
    ├── deploy-cloud-scheduler.yml        # Cloud Scheduler deployment
    └── deploy-dags.yml                   # Airflow DAG deployment
```

## Configuration Management

### Adding New Patterns
```sql
INSERT INTO `{project}.{audit_dataset}.raw_table_latency_thresholds` 
VALUES ('SourceName', 'InternalTableName', '%pattern%', 24);
```

### Excluding Tables
```sql
INSERT INTO `{project}.{audit_dataset}.ignore_latency_tables_list` 
VALUES ('dataset_name', 'table_name');
```

### Disabling Pattern Monitoring
```sql
UPDATE `{project}.{audit_dataset}.raw_table_latency_thresholds` 
SET latency_threshold = NULL 
WHERE table_pattern LIKE '%pattern%';
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Make your changes and commit (`git commit -am 'Add new feature'`)
4. Push to the branch (`git push origin feature/new-feature`)
5. Create a Pull Request
