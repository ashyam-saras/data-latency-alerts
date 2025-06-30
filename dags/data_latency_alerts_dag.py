"""
Data Latency Alerts DAG

This DAG orchestrates data latency monitoring by:
1. Executing BigQuery latency checks using utility functions
2. Converting results to CSV format
3. Sending CSV file to Slack channels

The DAG is scheduled to run twice daily at 6 AM and 6 PM IST.

REQUIREMENTS:
- BigQuery connection with permissions to query INFORMATION_SCHEMA
- Service account needs: BigQuery Data Viewer, BigQuery Job User
- Google Drive API must be enabled (for external table access to Google Sheets)
- Service account needs Google Drive read permissions for ignore_latency_tables_list table
- Slack connection with necessary scopes for file uploads and messaging
"""

import logging
import os

# Import our utility functions
import sys
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Add the current directory to Python path to find utils module
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from utils import (
    convert_results_to_csv,
    execute_bigquery_latency_check,
    send_failure_notification,
    send_latency_report_to_slack,
)

# Configuration from Airflow Variables
PROJECT_NAME = Variable.get("PROJECT_NAME", "insightsprod")
AUDIT_DATASET_NAME = Variable.get("AUDIT_DATASET_NAME", "edm_insights_metadata")
LOCATION = Variable.get("BIGQUERY_LOCATION", "us-central1")

# BigQuery connection configuration
BIGQUERY_CONN_ID = "data_latency_alerts__conn_id"

# Slack configuration from Airflow Variables
SLACK_CONN_ID = "slack_default"
SLACK_CHANNELS_STR = Variable.get("SLACK_CHANNELS", "#slack-bot-test")
# Split comma-separated channels and strip whitespace
SLACK_CHANNELS = [channel.strip() for channel in SLACK_CHANNELS_STR.split(",")]

# SQL file path (relative to DAGs bucket)
SQL_PATH = "sql/latency_check_query.sql"

# DAG Configuration
DAG_ID = "data_latency_alerts"
SCHEDULE_INTERVAL = "0 6,18 * * *"  # 6 AM and 6 PM daily (IST: 0:30 and 12:30 UTC)
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=0),
    "catchup": False,
}


def run_bigquery_latency_check(**context):
    """
    Execute BigQuery latency check and return results.
    """
    # Read SQL file content
    # Get the current DAG's directory (where this file is located)
    current_dag_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file_path = os.path.join(current_dag_dir, SQL_PATH)

    logging.info(f"📁 Looking for SQL file at: {sql_file_path}")

    with open(sql_file_path, "r") as file:
        sql_query = file.read()

    # Execute the query using our utility function
    results = execute_bigquery_latency_check(
        sql_query=sql_query,
        project_id=PROJECT_NAME,
        location=LOCATION,
        gcp_conn_id=BIGQUERY_CONN_ID,
        project_name=PROJECT_NAME,
        audit_dataset_name=AUDIT_DATASET_NAME,
        target_dataset=None,  # No specific dataset filter
    )

    return results


def convert_and_send_to_slack(**context):
    """
    Convert BigQuery results to CSV and send to Slack.
    """
    # Get results from the BigQuery task via XCom
    task_instance = context["task_instance"]
    results = task_instance.xcom_pull(task_ids="run_latency_check")

    # Convert to CSV
    csv_content = convert_results_to_csv(results)

    # Send to Slack
    execution_date = context.get("ds", "")
    send_latency_report_to_slack(
        csv_content=csv_content, channels=SLACK_CHANNELS, execution_date=execution_date, slack_conn_id=SLACK_CONN_ID
    )

    logging.info("✅ Report sent to Slack")
    return "success"


def notify_failure(**context):
    """
    Handle DAG failure notifications.
    """
    # Get the error from the context
    error_message = "DAG task failed - check logs for details"
    execution_date = context.get("ds", "")

    send_failure_notification(
        error_message=error_message,
        channels=SLACK_CHANNELS,
        dag_id=DAG_ID,
        execution_date=execution_date,
        slack_conn_id=SLACK_CONN_ID,
    )

    logging.info("Failure notification sent to Slack")
    return "failure_notified"


# Create the main DAG
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Simple BigQuery data latency monitoring with Slack notifications",
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["data-quality", "monitoring", "alerts", "bigquery", "slack"],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    # Task 1: Run the latency check using utility function
    latency_check_task = PythonOperator(
        task_id="run_latency_check",
        python_callable=run_bigquery_latency_check,
        provide_context=True,
    )

    # Task 2: Convert results to CSV and send to Slack
    send_report_task = PythonOperator(
        task_id="convert_and_send_to_slack",
        python_callable=convert_and_send_to_slack,
        provide_context=True,
    )

    # Task 3: Failure notification (if any task fails)
    notify_failure_task = PythonOperator(
        task_id="notify_failure",
        python_callable=notify_failure,
        provide_context=True,
        trigger_rule="one_failed",  # Only runs if upstream tasks fail
    )

    # Set up task dependencies
    latency_check_task >> send_report_task

    # Failure notification dependencies
    [latency_check_task, send_report_task] >> notify_failure_task
