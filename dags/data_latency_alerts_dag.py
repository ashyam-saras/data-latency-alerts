"""
Data Latency Alerts DAG

This DAG orchestrates the data latency monitoring process using Cloud Composer.
It consists of two main tasks:
1. Run latency check via Cloud Function
2. Handle results and send Slack notifications

Scheduled to run twice daily at 6 AM and 6 PM IST.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.utils.dates import days_ago

# Import Slack alerts
try:
    from utils.slack_alerts import on_failure_callback, on_success_callback
except ImportError:
    # Fallback if utils module is not available
    def on_success_callback(context):
        logging.info("Success callback - Slack alerts not available")

    def on_failure_callback(context):
        logging.error("Failure callback - Slack alerts not available")


# DAG Configuration
DAG_ID = "data_latency_alerts"
SCHEDULE_INTERVAL = "0 6,18 * * *"  # 6 AM and 6 PM daily
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "on_success_callback": on_success_callback,
    "on_failure_callback": on_failure_callback,
}

# Configuration from Airflow Variables
CLOUD_FUNCTION_URL = Variable.get("DATA_LATENCY_CLOUD_FUNCTION_URL")
SLACK_CHANNEL_ID = Variable.get("DATA_LATENCY_SLACK_CHANNEL_ID")
SLACK_API_TOKEN = Variable.get("DATA_LATENCY_SLACK_API_TOKEN")
PROJECT_NAME = Variable.get("DATA_LATENCY_PROJECT_NAME", "insightsprod")


@task(task_id="run_latency_check")
def run_latency_check(target_dataset: str = None) -> Dict[str, Any]:
    """
    Task to trigger the data latency check via Cloud Function.

    Args:
        target_dataset: Optional specific dataset to check

    Returns:
        Dict containing the response from the Cloud Function
    """
    logging.info(f"Starting latency check for project: {PROJECT_NAME}")

    # Prepare request payload
    payload = {
        "channel_id": SLACK_CHANNEL_ID,
        "slack_token": SLACK_API_TOKEN,
    }

    if target_dataset:
        payload["target_dataset"] = target_dataset
        logging.info(f"Targeting specific dataset: {target_dataset}")

    try:
        # Call the Cloud Function
        headers = {"Content-Type": "application/json"}

        logging.info(f"Calling Cloud Function: {CLOUD_FUNCTION_URL}")
        response = requests.post(
            CLOUD_FUNCTION_URL,
            json=payload,
            headers=headers,
            timeout=1800,  # 30 minutes timeout
        )

        response.raise_for_status()
        result = response.json()

        logging.info(f"Cloud Function response: {result}")
        return {
            "status": "success",
            "response": result,
            "payload": payload,
            "execution_time": datetime.now().isoformat(),
        }

    except requests.exceptions.RequestException as e:
        error_msg = f"Failed to call Cloud Function: {str(e)}"
        logging.error(error_msg)
        raise AirflowException(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error during latency check: {str(e)}"
        logging.error(error_msg)
        raise AirflowException(error_msg)


@task(task_id="process_results")
def process_results(latency_check_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Task to process the results from the latency check and log summary.

    Args:
        latency_check_result: Result from the latency check task

    Returns:
        Dict containing processing summary
    """
    logging.info("Processing latency check results")

    try:
        # Extract key information
        status = latency_check_result.get("status")
        response = latency_check_result.get("response", {})
        execution_time = latency_check_result.get("execution_time")

        # Log results
        if status == "success":
            logging.info("✅ Data latency check completed successfully")
            logging.info(f"Cloud Function response: {response.get('message', 'No message')}")

            # Log execution summary
            summary = {
                "dag_run": "success",
                "execution_time": execution_time,
                "cloud_function_status": response.get("status"),
                "cloud_function_message": response.get("message"),
            }

            logging.info(f"Execution Summary: {json.dumps(summary, indent=2)}")

        else:
            logging.warning("⚠️ Data latency check completed with issues")

        return {
            "processing_status": "completed",
            "original_result": latency_check_result,
            "processed_at": datetime.now().isoformat(),
        }

    except Exception as e:
        error_msg = f"Error processing results: {str(e)}"
        logging.error(error_msg)
        raise AirflowException(error_msg)


@task(task_id="handle_failures")
def handle_failures(context: Dict[str, Any]) -> None:
    """
    Task to handle failures and send error notifications.
    This task only runs if upstream tasks fail.

    Args:
        context: Airflow context containing task instance information
    """
    logging.error("🚨 Data latency alerts DAG failed")

    # Extract failure information
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")

    failure_info = {
        "dag_id": dag_run.dag_id if dag_run else "unknown",
        "execution_date": dag_run.execution_date.isoformat() if dag_run else "unknown",
        "task_id": task_instance.task_id if task_instance else "unknown",
        "state": task_instance.state if task_instance else "unknown",
    }

    logging.error(f"Failure details: {json.dumps(failure_info, indent=2)}")

    # TODO: Implement additional failure handling if needed
    # e.g., send alert to different Slack channel, email, etc.


# Create the DAG
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Automated data latency monitoring and alerting",
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["data-quality", "monitoring", "alerts"],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    # Task 1: Run the latency check
    latency_check_task = run_latency_check()

    # Task 2: Process the results
    process_task = process_results(latency_check_task)

    # Task 3: Handle failures (only runs on failure)
    failure_task = handle_failures()

    # Set up task dependencies
    latency_check_task >> process_task

    # Failure handling
    latency_check_task >> failure_task
    process_task >> failure_task


# Additional DAG for specific dataset monitoring (if needed)
@task(task_id="run_dataset_specific_check")
def run_dataset_specific_check(dataset_name: str) -> Dict[str, Any]:
    """
    Task to run latency check for a specific dataset.

    Args:
        dataset_name: Name of the specific dataset to check

    Returns:
        Dict containing the response from the Cloud Function
    """
    return run_latency_check(target_dataset=dataset_name)


# Create a separate DAG for ad-hoc dataset-specific checks
with DAG(
    dag_id=f"{DAG_ID}_dataset_specific",
    default_args=DEFAULT_ARGS,
    description="Ad-hoc data latency monitoring for specific datasets",
    schedule_interval=None,  # Manual trigger only
    tags=["data-quality", "monitoring", "alerts", "ad-hoc"],
    max_active_runs=3,
) as dataset_dag:

    # This DAG can be triggered manually with dataset parameter
    dataset_check = run_dataset_specific_check("{{ dag_run.conf.get('dataset_name', '') }}")
    dataset_process = process_results(dataset_check)
    dataset_failure = handle_failures()

    dataset_check >> dataset_process
    dataset_check >> dataset_failure
    dataset_process >> dataset_failure
