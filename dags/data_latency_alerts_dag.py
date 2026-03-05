"""
Data Latency Alerts DAG

This DAG orchestrates data latency monitoring by:
1. Collecting TABLE_STORAGE and SCHEMATA_OPTIONS metadata across all projects and regions
2. Running latency SQL query to build the failure details table
3. Executing latency check query on processed data
4. Converting results to XLSX format
5. Sending appropriate Slack notifications (success or failure) with client-specific routing

The DAG is scheduled to run once daily at 1:00 PM IST.

FEATURES:
- Rich Slack notifications with professional formatting
- Direct links to Airflow logs for failed tasks
- Single notification task handling both success and failure cases
- Automatic violation counting and smart templates
- Client-specific Slack channel routing based on dataset patterns (regex matching)

REQUIREMENTS:
- BigQuery connection with permissions to query processed tables
- Service account needs: BigQuery Data Viewer, BigQuery Job User
- Slack connection with necessary scopes for file uploads and messaging

AIRFLOW VARIABLES:
- LATENCY_ALERTS__PROJECT_NAME: GCP project name (default: insightsprod)
- LATENCY_ALERTS__AUDIT_DATASET_NAME: Metadata dataset name (default: edm_insights_metadata)
- LATENCY_ALERTS__BIGQUERY_LOCATION: BigQuery location (default: us-central1)
- LATENCY_ALERTS__SLACK_CHANNELS: Slack channel configuration (supports JSON formats)
- LATENCY_ALERTS__AIRFLOW_BASE_URL: Optional base URL for Airflow web UI (for DAG links, auto-detected for task logs)
"""

import base64
import json
import logging

# Import our utility functions
import sys
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

# Add the current directory to Python path to find utils module
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from utils import (
    collect_cross_region_metadata,
    convert_results_to_xlsx,
    execute_bigquery_latency_check,
    send_failure_notification,
    send_latency_report_to_slack,
    resolve_channels_for_results,
    parse_slack_channels_config,
    get_failure_channels,
)

# Configuration from Airflow Variables
# LATENCY_ALERTS__PROJECT_NAME is a JSON array of projects
PROJECTS_ARRAY = json.loads(Variable.get("LATENCY_ALERTS__PROJECT_NAME", '["insightsprod"]'))
# Use first project for single-project operations (BigQuery connection, dataset location, etc.)
PROJECT_NAME = PROJECTS_ARRAY[0] if PROJECTS_ARRAY else "insightsprod"
AUDIT_DATASET_NAME = Variable.get("LATENCY_ALERTS__AUDIT_DATASET_NAME", "edm_insights_metadata")
LOCATION = Variable.get("LATENCY_ALERTS__BIGQUERY_LOCATION", "us-central1")

# BigQuery connection configuration
BIGQUERY_CONN_ID = "data_latency_alerts__conn_id"

# Slack configuration from Airflow Variables
SLACK_CONN_ID = "slack_default"

# Parse Slack channels configuration (supports both JSON and comma-separated formats)
# Raises ValueError if configuration is invalid - this prevents DAG from loading with bad config
SLACK_CHANNELS_STR = Variable.get("LATENCY_ALERTS__SLACK_CHANNELS")
SLACK_CHANNEL_CONFIG = parse_slack_channels_config(SLACK_CHANNELS_STR)
# Backward compatibility: maintain SLACK_CHANNELS list for legacy code
SLACK_CHANNELS = SLACK_CHANNEL_CONFIG.get("channels", [SLACK_CHANNEL_CONFIG.get("default", "C065MG2L63U")])

logging.info(f"✅ Slack channel configuration loaded successfully")
if SLACK_CHANNEL_CONFIG.get("is_legacy"):
    logging.info(f"📝 Using legacy comma-separated format with {len(SLACK_CHANNELS)} channel(s)")
else:
    logging.info(
        f"📝 Using JSON format with {len(SLACK_CHANNEL_CONFIG.get('patterns', {}))} pattern(s) "
        f"and {len(SLACK_CHANNELS)} default channel(s)"
    )

# DAG Configuration
DAG_ID = "data_latency_alerts"
SCHEDULE_INTERVAL = "30 7 * * *"  # 1:00 PM IST (7:30 UTC)
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

def collect_metadata(**context):
    """
    Collect TABLE_STORAGE and SCHEMATA_OPTIONS from all projects and regions,
    then write to staging tables for the downstream SQL analysis query.
    """
    stats = collect_cross_region_metadata(
        projects=PROJECTS_ARRAY,
        dest_project=PROJECT_NAME,
        dest_dataset=AUDIT_DATASET_NAME,
        gcp_conn_id=BIGQUERY_CONN_ID,
    )
    logging.info(
        "📊 Metadata collection complete: %d storage rows, %d label rows",
        stats["table_storage_rows"],
        stats["dataset_labels_rows"],
    )
    return stats


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


def convert_to_xlsx(**context):
    """
    Convert BigQuery results to XLSX format.
    """
    # Get results from the latency check task via XCom
    task_instance = context["task_instance"]
    results = task_instance.xcom_pull(task_ids="run_latency_check") or []

    # Convert to XLSX
    xlsx_content = convert_results_to_xlsx(results)

    # Encode bytes to base64 string for XCom serialization
    xlsx_content_b64 = base64.b64encode(xlsx_content).decode("utf-8")

    logging.info(f"📄 Converted {len(results)} records to XLSX format")
    return {"xlsx_content_b64": xlsx_content_b64, "results": results}


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
        if "latency" in failed_task_id.lower():
            error_message = f"Latency check failed in task '{failed_task_id}'"
        elif "convert" in failed_task_id.lower():
            error_message = f"XLSX conversion failed in task '{failed_task_id}'"

        # Find the actual failed task instance
        failed_task_instance = None
        if dag_run:
            for ti in dag_run.get_task_instances():
                if ti.task_id == failed_task_id:
                    failed_task_instance = ti
                    break

        # Use failure channels (always defaults, regardless of pattern matching)
        failure_channels = get_failure_channels(SLACK_CHANNEL_CONFIG, SLACK_CHANNELS)
        
        send_failure_notification(
            error_message=error_message,
            channels=failure_channels,
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
        convert_data = task_instance.xcom_pull(task_ids="convert_to_xlsx")
        xlsx_content_b64 = convert_data["xlsx_content_b64"]
        results = convert_data["results"] or []

        # Decode base64 string back to bytes
        xlsx_content = base64.b64decode(xlsx_content_b64)

        # Resolve channels based on dataset patterns in results
        success_channels = resolve_channels_for_results(
            results=results,
            channel_config=SLACK_CHANNEL_CONFIG,
            fallback_channels=SLACK_CHANNELS,
        )

        send_latency_report_to_slack(
            xlsx_content=xlsx_content,
            results=results,
            channels=success_channels,
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
    description="Latency monitoring with Slack notifications",
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["data-quality", "monitoring", "alerts", "bigquery", "slack"],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    # Task 1: Collect metadata from all projects and regions into staging tables
    collect_metadata_task = PythonOperator(
        task_id="collect_metadata",
        python_callable=collect_metadata,
        provide_context=True,
    )

    # Task 2: Run latency SQL to rebuild the latency failure table
    run_latency_sql_task = BigQueryInsertJobOperator(
        task_id="run_latency_sql",
        gcp_conn_id=BIGQUERY_CONN_ID,
        configuration={
            "query": {
                "query": "{% include 'query.sql' %}",
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": PROJECT_NAME,
                    "datasetId": AUDIT_DATASET_NAME,
                    "tableId": "raw_table_latency_failure_details",
                },
            }
        },
        location=LOCATION,
    )

    # Task 3: Run latency check on processed data
    latency_check_task = PythonOperator(
        task_id="run_latency_check",
        python_callable=run_latency_check,
        provide_context=True,
    )

    # Task 4: Convert results to XLSX
    convert_xlsx_task = PythonOperator(
        task_id="convert_to_xlsx",
        python_callable=convert_to_xlsx,
        provide_context=True,
    )

    # Task 5: Send notification (handles both success and failure)
    notify_task = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Set up task dependencies
    collect_metadata_task >> run_latency_sql_task >> latency_check_task >> convert_xlsx_task >> notify_task

    # Ensure notification runs even if upstream tasks fail
    [collect_metadata_task, run_latency_sql_task, latency_check_task, convert_xlsx_task] >> notify_task
