[![Deploy Cloud Function](https://github.com/ashyam-saras/data-latency-alerts/actions/workflows/deploy-cloud-function.yml/badge.svg)](https://github.com/ashyam-saras/data-latency-alerts/actions/workflows/deploy-cloud-function.yml)
[![Deploy Cloud Scheduler](https://github.com/ashyam-saras/data-latency-alerts/actions/workflows/deploy-cloud-scheduler.yml/badge.svg)](https://github.com/ashyam-saras/data-latency-alerts/actions/workflows/deploy-cloud-scheduler.yml)
![Coverage](reports/coverage/badge.svg)

# Data Latency Alerts

This project provides an automated system to monitor and alert about BigQuery tables that have not been updated within a specified timeframe. It helps data teams maintain data freshness and quickly identify potential issues in data pipelines.

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
  - [Customizing Deployments](#customizing-deployments)
- [Project Structure](#project-structure)
- [Setting Up for Other GCP Projects](#setting-up-for-other-gcp-projects)
- [Maintenance](#maintenance)
- [Contributing](#contributing)

## Features

- Automated monitoring of BigQuery tables across multiple datasets
- Highly configurable monitoring parameters:
  - Dataset-level and table-specific configurations
  - Custom update thresholds for individual tables or datasets
  - Support for monitoring specific data segments (e.g., brands) within tables
- Parallel processing for efficient handling of large-scale data
- Slack notifications with summary statistics and detailed Excel reports
- Cloud Function deployment with scheduled execution via Cloud Scheduler
- Automated CI/CD pipeline using GitHub Actions

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

1. Configuration:
   - The system uses a configuration table in BigQuery (`LATENCY_PARAMS_TABLE`) to get monitoring parameters.
   - This table specifies which datasets and tables to monitor, their update thresholds, and any specific data segments to track.

2. Data Retrieval:
   - The Cloud Function queries BigQuery to get a list of tables based on the configuration.
   - It then checks the last update time of each table using the `__TABLES__` metadata.

3. Latency Check:
   - For each table or data segment, the system calculates the time since the last update.
   - If this time exceeds the specified threshold (which can be dataset-specific, table-specific, or segment-specific), the table or segment is flagged as outdated.

4. Parallel Processing:
   - To handle large numbers of datasets efficiently, the system processes multiple datasets in parallel.
   - The number of parallel workers is configurable via the `BQ_PARALLEL_DATASETS` environment variable.

5. Alert Generation:
   - If any tables are found to be outdated, an alert is generated.
   - The alert includes summary statistics like the total number of outdated tables, maximum delay, and average delay.
   - It also lists the top 5 datasets with the highest average delay.

6. Slack Notification:
   - The alert is formatted as a Slack message with structured blocks for easy readability.
   - A detailed Excel report of all outdated tables is generated and attached to the Slack message.

7. Scheduling:
   - The Cloud Function is triggered periodically by Cloud Scheduler.
   - The default schedule is every 6 hours, but this is configurable.

8. Error Handling and Logging:
   - The system includes comprehensive error handling and logging throughout the process.
   - Errors are caught, logged, and reported to ensure visibility of any issues.

## Setup and Configuration

### Environment Variables

Both workflows use GitHub Environments to manage environment-specific variables and secrets. The following variables are used:

- `PROJECT_NAME`: GCP project name (same for both environments)
- `AUDIT_DATASET_NAME`: Name of the audit dataset (same for both environments)
- `LATENCY_PARAMS_TABLE`: Name of the latency parameters table (different for dev and prod)
- `SLACK_CHANNEL_ID`: ID of the Slack channel for alerts (different for dev and prod)
- `SLACK_API_TOKEN`: Slack API token for sending messages (same for both environments)

These variables can be set in the GitHub repository settings under Environments > [Environment Name] > Environment variables.

### BigQuery Setup

1. Create an audit dataset (`AUDIT_DATASET_NAME`) in your BigQuery project.
2. In this dataset, create a table (`LATENCY_PARAMS_TABLE`) with the following schema:
   - `dataset` (STRING): Name of the dataset to monitor
   - `tables` (ARRAY<STRING>): Array of table names to monitor
   - `threshold_hours` (INTEGER): Number of hours after which a table is considered outdated
   - `group_by_column` (STRING): Column name to group by (e.g., 'brand')
   - `last_updated_column` (STRING): Column name for the last update timestamp

### Slack Setup

1. Create a Slack app in your workspace.
2. Get the Bot User OAuth Token and set it as `SLACK_API_TOKEN`.
3. Invite the bot to the channel you want to receive alerts in.
4. Get the channel ID and set it as `SLACK_CHANNEL_ID`.

## Deployment

This project uses GitHub Actions for automated deployments to both development and production environments. The deployment process is defined in two separate workflow files:

1. `deploy-cloud-function-dev.yml`: For deploying to the development environment
2. `deploy-cloud-function.yml`: For deploying to the production environment

### Development Deployment

The development deployment is triggered by:
- Pushing to the `dev` branch
- Manual workflow dispatch

### Production Deployment

The production deployment is triggered by:
- Pushing to the `main` branch
- Manual workflow dispatch

### Environment Variables

Both workflows use GitHub Environments to manage environment-specific variables and secrets. The following variables are used:

- `PROJECT_NAME`: GCP project name (same for both environments)
- `AUDIT_DATASET_NAME`: Name of the audit dataset (same for both environments)
- `LATENCY_PARAMS_TABLE`: Name of the latency parameters table (different for dev and prod)
- `SLACK_CHANNEL_ID`: ID of the Slack channel for alerts (different for dev and prod)
- `SLACK_API_TOKEN`: Slack API token for sending messages (same for both environments)

### Secrets

The following secrets are used in the deployment process and are set at the repository level:

- `GCP_SA_KEY`: The JSON key of the Google Cloud service account
- `SLACK_API_TOKEN`: The Slack Bot User OAuth Token

## Project Structure

The project is organized as follows:

- `main.py`: The entry point of the Cloud Function.
- `utils/`: A directory containing utility modules:
  - `bigquery.py`: Contains functions for interacting with BigQuery and processing latency data.
  - `logging.py`: Defines the `cprint` function for consistent logging throughout the project.
  - `slack.py`: Handles Slack message generation and sending.
- `tests/`: Contains test files for each module.
- `latency_check_dataset_level.sql`: SQL query used to check for latent tables at the dataset level.
- `latency_check_table_level.sql`: SQL query used to check for latent tables at the table level.
- `latency_check_group_by.sql`: SQL query used to check for latent tables with group-by configuration.
- `requirements.txt`: Lists all Python dependencies for the project.
- `.github/workflows/`: Contains GitHub Actions workflow files for CI/CD.

## Setting Up for Other GCP Projects

To set up this project for a different GCP project or client:

1. Fork this repository to your own GitHub account or organization.
2. In your forked repository, go to Settings > Secrets and variables > Actions.
3. Add the necessary repository secrets and variables.
4. Update the SQL files if you need to modify the query logic.
5. If needed, modify the Cloud Function and Cloud Scheduler deployment workflows in the `.github/workflows` directory to match your specific requirements.
6. Push your changes to the main branch to trigger the deployment.

## Maintenance

To maintain this system for multiple clients:

1. Create a separate fork for each client.
2. Set up the secrets and variables for each client's repository as described above.
3. If needed, create client-specific branches for customizations.
4. Use GitHub Actions to automate deployments for each client.
5. Regularly sync the forks with the main repository to get the latest updates and features.

## Contributing

Contributions to improve the project are welcome. Please follow these steps:

1. Fork the repository
2. Create a new branch (`git checkout -b feature-branch`)
3. Make your changes and commit (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin feature-branch`)
5. Create a new Pull Request
