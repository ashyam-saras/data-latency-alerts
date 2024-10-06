[![Deploy Cloud Function](https://github.com/ashyam-saras/data-latency-alerts/actions/workflows/deploy-cloud-function.yml/badge.svg)](https://github.com/ashyam-saras/data-latency-alerts/actions/workflows/deploy-cloud-function.yml)
[![Deploy Cloud Scheduler](https://github.com/ashyam-saras/data-latency-alerts/actions/workflows/deploy-cloud-scheduler.yml/badge.svg)](https://github.com/ashyam-saras/data-latency-alerts/actions/workflows/deploy-cloud-scheduler.yml)

# Data Latency Alerts

This project provides an automated system to monitor and alert about BigQuery tables that have not been updated within a specified timeframe. It helps data teams maintain data freshness and quickly identify potential issues in data pipelines.

## Features

- Monitors multiple datasets and tables in BigQuery
- Configurable update thresholds for each dataset
- Excludes specified tables from monitoring
- Sends alerts via Slack with detailed information
- Runs as a Cloud Function triggered by Cloud Scheduler
- Automated deployment using GitHub Actions

## How it works

1. The system queries a configuration table to get monitoring parameters for each dataset.
2. It then checks the last update time of tables in the specified datasets.
3. If any tables haven't been updated within the threshold, an alert is generated.
4. The alert is sent to a specified Slack channel with an attached Excel file containing details.

## Setup and Configuration

(Add instructions for setting up the project, including required permissions, environment variables, and configuration steps)

## Deployment

This project is automatically deployed using GitHub Actions. Any push to the main branch will trigger the deployment workflow.

