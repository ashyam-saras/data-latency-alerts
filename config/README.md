# Configuration Documentation

## Slack Alerts Configuration

### Configuration File: `slack_alerts.json`

This file configures Slack notifications for DAG success and failure events.

### Environment Variable Substitution

The configuration supports environment variable substitution using the syntax:
- `${VARIABLE_NAME}`: Substitutes with environment variable value
- `${VARIABLE_NAME:-default_value}`: Substitutes with environment variable or default value

### Required Environment Variables

Set these in your `.env` file or environment:

```bash
# Slack Configuration
SLACK_API_TOKEN=xoxb-your-slack-bot-token
SLACK_CHANNEL_ID=C06QY1KQJJG

# Optional: Separate channels for different alert types
SLACK_ERROR_CHANNEL_ID=C06QY1KQJJG      # Defaults to SLACK_CHANNEL_ID
SLACK_WARNING_CHANNEL_ID=C06QY1KQJJG    # Defaults to SLACK_CHANNEL_ID
```

### Configuration Structure

```json
{
  "slack_alerts": {
    "enabled": true,
    "default_token": "${SLACK_API_TOKEN}",
    "channels": {
      "success": {
        "channel_id": "${SLACK_CHANNEL_ID}",
        "token": "${SLACK_API_TOKEN}",
        "enabled": true,
        "message_template": "✅ Success message template"
      },
      "failure": {
        "channel_id": "${SLACK_ERROR_CHANNEL_ID:-${SLACK_CHANNEL_ID}}",
        "token": "${SLACK_API_TOKEN}",
        "enabled": true,
        "message_template": "🚨 Failure message template"
      }
    },
    "notification_settings": {
      "notify_on_success": true,
      "notify_on_failure": true,
      "notify_on_retry": false,
      "include_logs": true,
      "include_duration": true,
      "mention_users": {
        "on_failure": ["@data-engineering"],
        "on_success": []
      }
    }
  }
}
```

### Message Template Variables

Available variables for message templates:

**Success Messages:**
- `{dag_id}`: DAG identifier
- `{execution_date}`: Execution date/time
- `{duration}`: Task duration
- `{status}`: Execution status

**Failure Messages:**
- `{dag_id}`: DAG identifier
- `{execution_date}`: Execution date/time
- `{failed_task}`: Name of failed task
- `{error_message}`: Error message
- `{log_url}`: Link to task logs

### Customization Examples

#### Different Channels for Different Alert Types
```json
{
  "slack_alerts": {
    "channels": {
      "success": {
        "channel_id": "C06QY1KQJJG",
        "enabled": true
      },
      "failure": {
        "channel_id": "C07ZX2ALERT",
        "enabled": true
      }
    }
  }
}
```

#### Custom Message Templates
```json
{
  "slack_alerts": {
    "channels": {
      "success": {
        "message_template": "🎉 *{dag_id}* completed successfully!\n⏱️ Duration: {duration}\n📅 {execution_date}"
      },
      "failure": {
        "message_template": "💥 *{dag_id}* failed!\n❌ Task: {failed_task}\n🐛 Error: {error_message}\n🔗 <{log_url}|View Logs>"
      }
    }
  }
}
```

#### Disable Specific Notifications
```json
{
  "slack_alerts": {
    "notification_settings": {
      "notify_on_success": false,
      "notify_on_failure": true
    }
  }
}
```

### Deployment

The configuration file is automatically uploaded to your Composer environment when using the GitHub workflow. Ensure the `config/` directory is present in your repository. 

# Configuration Guide for Data Latency Alerts DAG

## Overview
This DAG uses BigQuery Data Transfer Service to process data and then monitors latency by querying the processed results.

## Required Airflow Variables

Set these variables in the Airflow UI under Admin > Variables:

### Core Configuration
```
PROJECT_NAME = insightsprod
AUDIT_DATASET_NAME = edm_insights_metadata
BIGQUERY_LOCATION = us-central1
```

### Data Transfer Service
```
LATENCY_ALERTS__BQ_DTS_CONFIG_ID = 68634e9e-0000-2fb5-9841-ac3eb1471eb8
```
**Note**: This must be a transfer config created in the `us-central1` location

### Slack Configuration
```
SLACK_CHANNELS = #data-alerts,#monitoring
AIRFLOW_BASE_URL = https://your-airflow-instance.com  # Optional, for DAG links
```

## Required Airflow Connections

### BigQuery Connection: `data_latency_alerts__conn_id`
- **Connection Type**: Google Cloud
- **Project ID**: Your GCP project
- **Keyfile JSON**: Service account key with permissions:
  - BigQuery Data Viewer
  - BigQuery Job User
  - BigQuery Data Transfer Admin

### Slack Connection: `slack_default`
- **Connection Type**: Slack Webhook
- **Host**: hooks.slack.com
- **Password**: Your Bot User OAuth Token (xoxb-...)
- **Required Scopes**:
  - `chat:write`
  - `chat:write.customize`
  - `files:write`
  - `files:read`
  - `channels:read`
  - `groups:read` (for private channels)

## DAG Workflow

1. **Start Data Transfer**: Triggers BigQuery DTS job and waits for completion
2. **Run Latency Check**: Queries `raw_table_latency_failure_details` table
3. **Convert to CSV**: Processes results for Slack attachment
4. **Send Notification**: Sends success/failure notification to Slack

## Table Requirements

The DAG expects the following table to exist after the data transfer:
```sql
`{PROJECT_NAME}.{AUDIT_DATASET_NAME}.raw_table_latency_failure_details`
```

This table should contain latency violation records with appropriate columns for CSV export.

## Troubleshooting

### Common Issues
1. **Transfer Config Not Found**: 
   - Verify `LATENCY_ALERTS__BQ_DTS_CONFIG_ID` variable is set correctly
   - Ensure the transfer config exists in the `us-central1` location
   - Check that the config ID format is correct (UUID format)
2. **Permission Errors**: Ensure service account has BigQuery Data Transfer Admin role
3. **Slack API Errors**: Check connection scopes and bot permissions
4. **Table Not Found**: Verify the data transfer creates the expected table

### Monitoring
- DAG runs twice daily at 6 AM and 6 PM IST
- Check Airflow logs for detailed error messages
- Slack notifications include direct links to failed task logs 