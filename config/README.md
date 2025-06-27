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