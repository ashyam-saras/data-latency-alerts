# Cloud Composer Orchestration for Data Latency Alerts

This directory contains the Cloud Composer (Apache Airflow) orchestration setup for the Data Latency Alerts system.

## Overview

The orchestration consists of:
- **Main DAG**: `data_latency_alerts` - Runs twice daily (6 AM and 6 PM IST)
- **Ad-hoc DAG**: `data_latency_alerts_dataset_specific` - Manual trigger for specific datasets
- **Deployment Scripts**: Automated setup and deployment utilities

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Cloud         │    │   Cloud         │    │   Slack         │
│   Composer      │───▶│   Function      │───▶│   Notification  │
│   (Airflow)     │    │   (Latency      │    │   + Excel       │
│                 │    │   Check)        │    │   Report        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## DAG Structure

### Main DAG: `data_latency_alerts`
- **Schedule**: `0 6,18 * * *` (6 AM and 6 PM daily)
- **Tasks**:
  1. `run_latency_check` - Triggers the Cloud Function
  2. `process_results` - Processes and logs the results
  3. `handle_failures` - Handles any failures and errors

### Dataset-Specific DAG: `data_latency_alerts_dataset_specific`
- **Schedule**: Manual trigger only
- **Purpose**: Run latency checks for specific datasets
- **Usage**: Trigger with `{"dataset_name": "your_dataset_name"}` configuration

## Quick Start

### 1. Setup Cloud Composer Environment

```bash
# Make scripts executable
chmod +x composer/setup_composer.sh
chmod +x composer/deploy_dags.sh

# Create Composer environment (takes 15-20 minutes)
cd composer
./setup_composer.sh [ENVIRONMENT_NAME] [LOCATION]

# Example:
./setup_composer.sh data-latency-composer us-central1
```

### 2. Deploy DAGs

```bash
# Deploy DAGs to the Composer environment
./deploy_dags.sh [ENVIRONMENT_NAME] [LOCATION]

# Example:
./deploy_dags.sh data-latency-composer us-central1
```

### 3. Configure Airflow Variables

Set the following variables in the Airflow UI or using gcloud commands:

```bash
# Using gcloud (replace with your actual values)
ENVIRONMENT_NAME="data-latency-composer"
LOCATION="us-central1"
PROJECT_ID="insightsprod"

gcloud composer environments run $ENVIRONMENT_NAME --location $LOCATION variables set -- \
    DATA_LATENCY_CLOUD_FUNCTION_URL "https://us-central1-$PROJECT_ID.cloudfunctions.net/data-latency-alerts"

gcloud composer environments run $ENVIRONMENT_NAME --location $LOCATION variables set -- \
    DATA_LATENCY_SLACK_CHANNEL_ID "C06QY1KQJJG"

gcloud composer environments run $ENVIRONMENT_NAME --location $LOCATION variables set -- \
    DATA_LATENCY_SLACK_API_TOKEN "xoxb-your-slack-token"

gcloud composer environments run $ENVIRONMENT_NAME --location $LOCATION variables set -- \
    DATA_LATENCY_PROJECT_NAME "$PROJECT_ID"
```

### 4. Enable DAGs

1. Go to the Airflow UI (URL provided after environment creation)
2. Enable the `data_latency_alerts` DAG
3. Optionally enable the `data_latency_alerts_dataset_specific` DAG

## Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DATA_LATENCY_CLOUD_FUNCTION_URL` | URL of the deployed Cloud Function | `https://us-central1-project.cloudfunctions.net/data-latency-alerts` |
| `DATA_LATENCY_SLACK_CHANNEL_ID` | Slack channel ID for notifications | `C06QY1KQJJG` |
| `DATA_LATENCY_SLACK_API_TOKEN` | Slack API token | `xoxb-xxx-xxx` |
| `DATA_LATENCY_PROJECT_NAME` | GCP project name | `insightsprod` |

### DAG Configuration

You can modify the DAG configuration in `dags/data_latency_alerts_dag.py`:

```python
# Schedule (currently 6 AM and 6 PM daily)
SCHEDULE_INTERVAL = "0 6,18 * * *"

# Timeout for Cloud Function calls
timeout=1800  # 30 minutes

# Retry configuration
"retries": 2,
"retry_delay": timedelta(minutes=5),
```

## Manual Execution

### Run Main DAG Manually

```bash
# Trigger the main DAG
gcloud composer environments run $ENVIRONMENT_NAME --location $LOCATION dags trigger -- data_latency_alerts
```

### Run Dataset-Specific Check

```bash
# Trigger with specific dataset
gcloud composer environments run $ENVIRONMENT_NAME --location $LOCATION dags trigger -- \
    data_latency_alerts_dataset_specific --conf '{"dataset_name": "your_dataset_name"}'
```

## Monitoring and Troubleshooting

### View DAG Status

1. **Airflow UI**: Access via the URL provided during setup
2. **Logs**: Check task logs in the Airflow UI for detailed execution information
3. **Cloud Function Logs**: Check Cloud Function logs for latency check details

### Common Issues

1. **Cloud Function Timeout**: Increase timeout in DAG configuration
2. **Authentication Issues**: Verify service account permissions
3. **Slack Notifications**: Check Slack token and channel ID validity

### Useful Commands

```bash
# Check environment status
gcloud composer environments describe $ENVIRONMENT_NAME --location $LOCATION

# View DAG runs
gcloud composer environments run $ENVIRONMENT_NAME --location $LOCATION dags list

# Check task logs
gcloud composer environments run $ENVIRONMENT_NAME --location $LOCATION tasks log -- \
    data_latency_alerts run_latency_check 2024-01-01

# Update Python packages
gcloud composer environments update $ENVIRONMENT_NAME --location $LOCATION \
    --update-pypi-packages-from-file=requirements.txt
```

## Cost Optimization

### Environment Sizing
- **Development**: `n1-standard-1` with 3 nodes
- **Production**: `n1-standard-2` with 3-5 nodes

### Auto-scaling
```bash
# Enable auto-scaling (optional)
gcloud composer environments update $ENVIRONMENT_NAME --location $LOCATION \
    --enable-autoscaling \
    --min-workers 1 \
    --max-workers 3
```

## Security

### Network Security
- Environment uses private IP with authorized networks
- Master CIDR: `172.16.0.0/23`

### IAM Permissions
Required roles for the service account:
- `roles/composer.worker`
- `roles/cloudfunctions.invoker`
- `roles/bigquery.jobUser`

## File Structure

```
composer/
├── README.md                           # This file
├── setup_composer.sh                   # Environment setup script
├── deploy_dags.sh                      # DAG deployment script
├── requirements.txt                    # Python dependencies
└── ../dags/
    └── data_latency_alerts_dag.py     # Main DAG file
```

## Next Steps

1. **Monitor Performance**: Track DAG execution times and optimize as needed
2. **Add Alerting**: Configure email alerts for DAG failures
3. **Extend Functionality**: Add more datasets or custom logic as needed
4. **Cost Monitoring**: Set up billing alerts for Composer usage

## Support

For issues or questions:
1. Check Airflow UI logs
2. Review Cloud Function logs
3. Verify configuration variables
4. Check network connectivity and permissions 