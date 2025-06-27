[![Deploy Airflow DAGs](https://github.com/ashyam-saras/data-latency-alerts/actions/workflows/deploy-dags.yml/badge.svg)](https://github.com/ashyam-saras/data-latency-alerts/actions/workflows/deploy-dags.yml)
![Coverage](reports/coverage/badge.svg)

# Data Latency Alerts

A comprehensive data latency monitoring system that executes BigQuery queries directly in Airflow to detect and alert on data freshness issues across multiple datasets.

## 🏗️ Architecture Overview

The system uses a **pattern-based monitoring approach** with native Airflow BigQuery operators:

1. **Native BigQuery Integration**: Uses [`BigQueryInsertJobOperator`](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html#execute-bigquery-jobs) for optimal Airflow integration
2. **Pattern Matching**: Uses `raw_table_latency_thresholds` for flexible table pattern matching
3. **Airflow Orchestration**: Scheduled DAGs with deferrable operators for resource efficiency
4. **Slack Notifications**: Configurable alerts for success/failure/warnings
5. **Dataset Filtering**: Supports both inclusion and exclusion patterns

## 📋 Features

- **Native Airflow Integration**: Uses official `BigQueryInsertJobOperator` and [`SlackAPIPostOperator`](https://airflow.apache.org/docs/apache-airflow-providers-slack/stable/_api/airflow/providers/slack/operators/slack/index.html#airflow.providers.slack.operators.slack.SlackAPIPostOperator) with deferrable mode
- **Pattern-Based Configuration**: Flexible table matching using SQL patterns
- **Intelligent Filtering**: Excludes datasets and tables based on labels and lists
- **Native Slack Notifications**: Rich Slack messages with emojis, templating, and conditional channels
- **Flexible Scheduling**: Multiple DAGs for different monitoring needs
- **Resource Efficient**: Deferrable operators for better resource management
- **Zero Dependencies**: All required packages pre-installed in Cloud Composer
- **Airflow Variables Support**: Configuration via Airflow Variables with smart defaults

## 🔧 Configuration

### Required Airflow Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `PROJECT_NAME` | GCP project name | `insightsprod` | ✅ |
| `AUDIT_DATASET_NAME` | Metadata dataset name | `edm_insights_metadata` | ✅ |
| `BIGQUERY_LOCATION` | BigQuery region | `us-central1` | ❌ |
| `SLACK_CHANNEL_ID` | Default Slack channel | `#data-alerts` | ❌ |
| `SLACK_SUCCESS_CHANNEL_ID` | Success notifications channel | Falls back to `SLACK_CHANNEL_ID` | ❌ |
| `SLACK_FAILURE_CHANNEL_ID` | Failure notifications channel | Falls back to `SLACK_CHANNEL_ID` | ❌ |

### Required Airflow Connections

| Connection ID | Type | Description |
|---------------|------|-------------|
| `slack_default` | Slack | Slack API token for notifications |

#### Slack Connection Setup
```bash
# In Airflow UI: Admin → Connections → Create
Connection Id: slack_default
Connection Type: Slack
Password: xoxb-your-slack-bot-token
```

### Configuration Hierarchy

The system supports multiple configuration methods in order of precedence:

1. **Airflow Variables** (highest priority)
2. **JSON Configuration File** (`config/slack_alerts.json`)
3. **Environment Variables** (fallback)

### Example Airflow Variables Setup

```bash
# In Airflow UI or CLI
airflow variables set PROJECT_NAME "insightsprod"
airflow variables set AUDIT_DATASET_NAME "edm_insights_metadata"
airflow variables set BIGQUERY_LOCATION "us-central1"
airflow variables set SLACK_CHANNEL_ID "#data-alerts"
airflow variables set SLACK_SUCCESS_CHANNEL_ID "#data-success"
airflow variables set SLACK_FAILURE_CHANNEL_ID "#data-errors"
```

## 🚀 Deployment

### Automatic Deployment

The system automatically deploys to a dedicated `data-latency-alerts/` subfolder in your existing Cloud Composer environment:

```bash
# Push to main branch triggers deployment
git push origin main
```

Files are deployed to: `gs://your-dag-bucket/data-latency-alerts/`

### Manual Deployment

Trigger deployment manually via GitHub Actions:

1. Go to **Actions** → **Deploy Airflow DAGs**
2. Click **Run workflow**
3. Optionally specify:
   - Custom DAG bucket path
   - Environment name
   - Location

## 📊 DAGs Overview

### 1. `data_latency_alerts` (Main DAG)

- **Schedule**: Twice daily at 6 AM and 6 PM IST
- **Purpose**: Monitors all `*_prod_raw` datasets for latency violations
- **Tasks**:
  - `run_latency_check`: Execute BigQuery pattern-based query
  - `process_results`: Process and log results
  - `handle_failures`: Handle any failures

### 2. `data_latency_alerts_dataset_specific` (Ad-hoc DAG)

- **Schedule**: Manual trigger only
- **Purpose**: Monitor specific datasets on-demand
- **Usage**: 
  ```bash
  airflow dags trigger data_latency_alerts_dataset_specific \
    --conf '{"dataset_name": "your_dataset_prod_raw"}'
  ```

## 🔍 Monitoring Logic

### Pattern-Based Query

The system uses a sophisticated SQL query that:

1. **Matches Tables**: Uses `raw_table_latency_thresholds` for pattern matching
2. **Checks Freshness**: Compares `STORAGE_LAST_MODIFIED_TIME` with thresholds
3. **Applies Filters**: Excludes datasets/tables based on:
   - Dataset labels (`latency_check_ignore=true`)
   - Table exclusion list (`ignore_latency_tables_list`)
4. **Focuses on Production**: Only monitors `*_prod_raw` datasets

### Key Components

- **Patterns Table**: `{project}.{audit_dataset}.raw_table_latency_thresholds`
- **Exclusion List**: `{project}.{audit_dataset}.ignore_latency_tables_list`
- **Data Source**: `INFORMATION_SCHEMA.TABLE_STORAGE` for accurate timestamps

## 📱 Slack Notifications

### Success Notifications

- ✅ **No Violations**: Clean bill of health
- ⚠️ **Violations Found**: Summary with top affected datasets

### Failure Notifications

- 🚨 **DAG Failures**: Task failures with error details
- 📋 **Error Context**: Detailed debugging information

### Message Format

```
✅ Data Latency Check Completed Successfully
📊 DAG: data_latency_alerts
⏰ Execution Date: 2024-01-15
🕐 Duration: 2m 15s
📈 Status: ✅ No violations
```

## 🔧 Development

### Local Development

1. **Clone Repository**:
   ```bash
   git clone https://github.com/your-org/data-latency-alerts.git
   cd data-latency-alerts
   ```

2. **Install Dependencies** (for local testing):
   ```bash
   # Install Airflow and providers for local development
   pip install apache-airflow apache-airflow-providers-google slack-sdk pandas
   ```

3. **Set Environment Variables** (for local testing):
   ```bash
   export PROJECT_NAME="your-project"
   export AUDIT_DATASET_NAME="your-metadata-dataset"
   export SLACK_CHANNEL_ID="your-channel"
   # Note: Slack token configured via Airflow Connection in production
   ```

### Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html

# Test specific components
pytest tests/test_bigquery.py
pytest tests/test_slack.py
```

## 📁 Project Structure

### Repository Structure
```
data-latency-alerts/
├── dags/
│   ├── data_latency_alerts_dag.py       # Main DAG with native operators
│   ├── sql/
│   │   └── latency_check_query.sql     # SQL query with Jinja templating
│   └── utils/
│       └── __init__.py                 # Python package marker
├── config/
│   ├── slack_alerts.json               # Slack configuration (optional)
│   └── README.md                       # Config documentation
├── .github/
│   └── workflows/
│       └── deploy-dags.yml             # Deployment workflow
├── tests/                              # Test suite
├── insightsprod-8b1340c52f9d.json     # Service account key
├── pytest.ini                         # Test configuration
└── README.md                          # This file
```

### Deployed Structure (in Composer DAG bucket)
```
gs://your-dag-bucket/data-latency-alerts/
├── data_latency_alerts_dag.py           # Main DAG file
├── sql/
│   └── latency_check_query.sql         # SQL query
├── utils/
│   └── __init__.py                     # Package marker
└── config/
    └── slack_alerts.json               # Configuration (optional)
```

## 🚨 Troubleshooting

### Common Issues

1. **BigQuery Access**: Ensure Composer service account has BigQuery permissions
2. **Slack Permissions**: Verify bot token has `chat:write` and `channels:read` scopes
3. **Airflow Variables**: Check all required variables are set correctly
4. **SQL File Access**: Ensure `sql/latency_check_query.sql` is deployed to the DAGs bucket
5. **Provider Package**: Verify `apache-airflow-providers-google` is available in Composer (default in v2.x)

### Debugging

1. **Check Logs**: View Airflow task logs for detailed error messages
2. **Test Variables**: Verify Airflow Variables are accessible
3. **Validate Patterns**: Ensure `raw_table_latency_thresholds` table exists
4. **Check Permissions**: Verify service account permissions

## 🔄 Migration from Cloud Function

If migrating from the previous Cloud Function approach:

1. **Architecture Change**: Cloud Function → Native Airflow BigQuery operators
2. **Project Structure**: All code consolidated into `dags/` folder
3. **Remove Variables**: Delete `DATA_LATENCY_CLOUD_FUNCTION_URL`
4. **Update Variables**: Rename variables to new format (see configuration section)
5. **Deploy**: Push changes to trigger deployment
6. **Verify**: Check DAGs appear in Airflow UI
7. **Clean up**: Remove Cloud Function and Cloud Scheduler resources (no longer needed)

### What Changed
- ✅ **Simplified**: Single repository, all in `dags/` folder
- ✅ **Native Integration**: Uses `BigQueryInsertJobOperator` with deferrable mode
- ✅ **Better Resource Management**: No external HTTP calls or timeouts
- ✅ **SQL in Files**: Query logic separated from Python code
- ✅ **Legacy Support**: Old utility files preserved for reference

## 📈 Performance

- **Execution Time**: Typically 1-3 minutes for full scan
- **Resource Usage**: Minimal - single BigQuery query
- **Scalability**: Handles thousands of tables efficiently
- **Cost**: Optimized BigQuery queries for minimal cost

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Need Help?** Check the [troubleshooting section](#🚨-troubleshooting) or create an issue in the repository.
