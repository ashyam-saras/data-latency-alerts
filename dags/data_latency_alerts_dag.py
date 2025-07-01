"""
Data Latency Alerts DAG

This DAG orchestrates data latency monitoring by:
1. Triggering BigQuery Data Transfer Service job (waits for completion)
2. Executing simple latency check query on processed data
3. Converting results to CSV format
4. Sending appropriate Slack notifications (success or failure)

The DAG is scheduled to run twice daily at 6 AM and 6 PM IST.

FEATURES:
- BigQuery Data Transfer Service integration
- Rich Slack notifications with professional formatting
- Direct links to Airflow logs for failed tasks
- Single notification task handling both success and failure cases
- Automatic violation counting and smart templates

REQUIREMENTS:
- BigQuery Data Transfer Service permissions
- BigQuery connection with permissions to query processed tables
- Service account needs: BigQuery Data Viewer, BigQuery Job User, BigQuery Data Transfer Admin
- Slack connection with necessary scopes for file uploads and messaging

AIRFLOW VARIABLES:
- LATENCY_ALERTS__PROJECT_NAME: GCP project name (default: insightsprod)
- LATENCY_ALERTS__AUDIT_DATASET_NAME: Metadata dataset name (default: edm_insights_metadata)
- LATENCY_ALERTS__BIGQUERY_LOCATION: BigQuery location (default: us-central1)
- LATENCY_ALERTS__SLACK_CHANNELS: Comma-separated Slack channels (default: #slack-bot-test)
- LATENCY_ALERTS__AIRFLOW_BASE_URL: Optional base URL for Airflow web UI (for DAG links, auto-detected for task logs)
- LATENCY_ALERTS__BQ_DTS_CONFIG_ID: BigQuery DTS transfer configuration ID (must be in us-central1)
"""

import logging

# Import our utility functions
import sys
import time
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery_dts import BigQueryDataTransferServiceStartTransferRunsOperator
from airflow.utils.dates import days_ago
from airflow.utils.state import State

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
PROJECT_NAME = Variable.get("LATENCY_ALERTS__PROJECT_NAME", "insightsprod")
AUDIT_DATASET_NAME = Variable.get("LATENCY_ALERTS__AUDIT_DATASET_NAME", "edm_insights_metadata")
LOCATION = Variable.get("LATENCY_ALERTS__BIGQUERY_LOCATION", "us-central1")
TRANSFER_CONFIG_ID = Variable.get("LATENCY_ALERTS__BQ_DTS_CONFIG_ID")

# BigQuery connection configuration
BIGQUERY_CONN_ID = "data_latency_alerts__conn_id"

# Slack configuration from Airflow Variables
SLACK_CONN_ID = "slack_default"
SLACK_CHANNELS_STR = Variable.get("LATENCY_ALERTS__SLACK_CHANNELS", "#slack-bot-test")
# Split comma-separated channels and strip whitespace
SLACK_CHANNELS = [channel.strip() for channel in SLACK_CHANNELS_STR.split(",")]

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


def run_latency_check(**context):
    """
    Execute simplified latency check query on processed data.
    """
    # Simple SQL query - no file needed
    sql_query = (
        "SELECT * FROM `{{ params.project_name }}.{{ params.audit_dataset_name }}.raw_table_latency_failure_details`"
    )

    logging.info(f"🔍 Executing latency check query")

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

    logging.info(f"📊 Latency check completed. Found {len(results)} records")
    return results


def convert_to_csv(**context):
    """
    Convert BigQuery results to CSV format.
    """
    # Get results from the latency check task via XCom
    task_instance = context["task_instance"]
    results = task_instance.xcom_pull(task_ids="run_latency_check")

    # Convert to CSV
    csv_content = convert_results_to_csv(results)

    logging.info(f"📄 Converted {len(results)} records to CSV format")
    return csv_content


def send_notification(**context):
    """
    Send appropriate notification based on upstream task states.
    Handles both success and failure scenarios.
    """
    dag_run = context.get("dag_run")
    task_instance = context["task_instance"]
    execution_date = context.get("ds", "")

    # Check if any upstream tasks failed
    failed_tasks = []
    success_tasks = []

    if dag_run:
        task_instances = dag_run.get_task_instances()
        for ti in task_instances:
            if ti.task_id != "send_notification":  # Exclude self
                if ti.state == State.FAILED:
                    failed_tasks.append(ti.task_id)
                elif ti.state == State.SUCCESS:
                    success_tasks.append(ti.task_id)

    # If any task failed, send failure notification
    if failed_tasks:
        failed_task_id = failed_tasks[0]  # Use first failed task
        error_message = f"Task '{failed_task_id}' failed - check logs for details"

        # Try to get more specific error context
        if "transfer" in failed_task_id.lower():
            error_message = f"BigQuery Data Transfer failed in task '{failed_task_id}'"
        elif "latency" in failed_task_id.lower():
            error_message = f"Latency check failed in task '{failed_task_id}'"
        elif "convert" in failed_task_id.lower():
            error_message = f"CSV conversion failed in task '{failed_task_id}'"

        # Find the actual failed task instance
        failed_task_instance = None
        if dag_run:
            for ti in dag_run.get_task_instances():
                if ti.task_id == failed_task_id:
                    failed_task_instance = ti
                    break

        send_failure_notification(
            error_message=error_message,
            channels=SLACK_CHANNELS,
            dag_id=DAG_ID,
            execution_date=execution_date,
            failed_task_id=failed_task_id,
            task_instance=failed_task_instance or task_instance,
            slack_conn_id=SLACK_CONN_ID,
        )

        logging.info(f"❌ Failure notification sent for task: {failed_task_id}")
        return "failure_notified"

    else:
        # All tasks succeeded, send success report
        csv_content = task_instance.xcom_pull(task_ids="convert_to_csv")

        send_latency_report_to_slack(
            csv_content=csv_content,
            channels=SLACK_CHANNELS,
            execution_date=execution_date,
            dag_id=DAG_ID,
            task_instance=task_instance,
            slack_conn_id=SLACK_CONN_ID,
        )

        logging.info("✅ Success report sent to Slack")
        return "success_notified"


# Create the main DAG
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="BigQuery Data Transfer + Latency monitoring with Slack notifications",
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["data-quality", "monitoring", "alerts", "bigquery", "slack", "data-transfer"],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    # Task 1: Trigger BigQuery Data Transfer Service
    start_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id="start_data_transfer",
        project_id=PROJECT_NAME,
        location=LOCATION,
        transfer_config_id=TRANSFER_CONFIG_ID,
        requested_run_time={"seconds": int(time.time() + 60)},  # Start in 1 minute
        gcp_conn_id=BIGQUERY_CONN_ID,
    )

    # Task 2: Run latency check on processed data
    latency_check_task = PythonOperator(
        task_id="run_latency_check",
        python_callable=run_latency_check,
        provide_context=True,
    )

    # Task 3: Convert results to CSV
    convert_csv_task = PythonOperator(
        task_id="convert_to_csv",
        python_callable=convert_to_csv,
        provide_context=True,
    )

    # Task 4: Send notification (handles both success and failure)
    notify_task = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification,
        provide_context=True,
        trigger_rule="none_skipped",  # Run regardless of upstream success/failure, as long as not skipped
    )

    # Set up task dependencies
    start_transfer >> latency_check_task >> convert_csv_task >> notify_task

    # Ensure notification runs even if upstream tasks fail
    [start_transfer, latency_check_task, convert_csv_task] >> notify_task
