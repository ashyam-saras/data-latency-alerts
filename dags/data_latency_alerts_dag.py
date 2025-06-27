"""
Data Latency Alerts DAG

This DAG orchestrates data latency monitoring by:
1. Executing BigQuery latency checks using native operators
2. Saving results as CSV file
3. Sending CSV file to Slack with SlackAPIFileOperator

The DAG is scheduled to run twice daily at 6 AM and 6 PM IST.
"""

import logging
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.slack.operators.slack import SlackAPIFileOperator
from airflow.utils.dates import days_ago

# DAG Configuration
DAG_ID = "data_latency_alerts"
SCHEDULE_INTERVAL = "0 6,18 * * *"  # 6 AM and 6 PM daily (IST: 0:30 and 12:30 UTC)
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

# Configuration from Airflow Variables
PROJECT_NAME = Variable.get("PROJECT_NAME", "insightsprod")
AUDIT_DATASET_NAME = Variable.get("AUDIT_DATASET_NAME", "edm_insights_metadata")
LOCATION = Variable.get("BIGQUERY_LOCATION", "us-central1")

# Slack configuration from Airflow Variables
SLACK_CONN_ID = "slack_default"
SLACK_CHANNEL_ID = Variable.get("SLACK_CHANNEL_ID", "#data-alerts")

# SQL file path (relative to DAGs bucket)
SQL_PATH = "sql/latency_check_query.sql"


@task(task_id="convert_results_to_csv")
def convert_results_to_csv(**context) -> str:
    """
    Convert BigQuery results to CSV format for Slack file upload.

    Returns:
        str: CSV content as string
    """
    import csv
    import io

    logging.info("Converting BigQuery results to CSV")

    try:
        # Get results from the BigQuery task via XCom
        task_instance = context["task_instance"]
        results = task_instance.xcom_pull(task_ids="run_latency_check")

        if not results:
            # Create CSV with header and no violations message
            csv_content = "message\nNo latency violations found - all tables are up to date! ✅"
            logging.info("✅ No latency violations found")
            return csv_content

        # Convert results to CSV format
        output = io.StringIO()

        if results:
            # Get field names from first row
            fieldnames = list(results[0].keys())
            writer = csv.DictWriter(output, fieldnames=fieldnames)

            # Write header and data
            writer.writeheader()
            writer.writerows(results)

        csv_content = output.getvalue()
        output.close()

        total_violations = len(results)
        logging.info(f"📊 Generated CSV with {total_violations} latency violations")

        return csv_content

    except Exception as e:
        error_msg = f"Error converting results to CSV: {str(e)}"
        logging.error(error_msg)

        # Return error CSV
        csv_content = f"error\n{error_msg}"
        return csv_content


# Create the main DAG
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Simplified BigQuery data latency monitoring with CSV file upload to Slack",
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["data-quality", "monitoring", "alerts", "bigquery", "slack"],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    # Task 1: Run the latency check using native BigQuery operator
    latency_check_task = BigQueryInsertJobOperator(
        task_id="run_latency_check",
        configuration={
            "query": {
                "query": "{% include '" + SQL_PATH + "' %}",
                "useLegacySql": False,
            }
        },
        params={
            "project_name": PROJECT_NAME,
            "audit_dataset_name": AUDIT_DATASET_NAME,
            "target_dataset": None,  # No specific dataset filter for main DAG
        },
        location=LOCATION,
        project_id=PROJECT_NAME,
        deferrable=True,  # Use deferrable mode for better resource efficiency
    )

    # Task 2: Convert results to CSV
    csv_task = convert_results_to_csv()

    # Task 3: Send CSV file to Slack
    send_csv_to_slack = SlackAPIFileOperator(
        task_id="send_csv_to_slack",
        slack_conn_id=SLACK_CONN_ID,
        channels=SLACK_CHANNEL_ID,
        initial_comment="🔍 **Data Latency Check Results** 📊\n"
        "Here are the latest latency monitoring results. "
        "Please find the detailed report in the attached CSV file.",
        filename="data_latency_check_{{ ds }}.csv",
        filetype="csv",
        content="{{ ti.xcom_pull(task_ids='convert_results_to_csv') }}",
        title="Data Latency Check Report - {{ ds }}",
    )

    # Task 4: Failure notification (if any task fails)
    notify_failure = SlackAPIFileOperator(
        task_id="notify_failure",
        slack_conn_id=SLACK_CONN_ID,
        channels=SLACK_CHANNEL_ID,
        initial_comment="🚨 **Data Latency Check DAG Failed** 🚨\n"
        f"📊 DAG: `{DAG_ID}`\n"
        "⏰ Execution Date: {{ ds }}\n"
        "❌ Check Airflow logs for details",
        filename="dag_failure_log_{{ ds }}.txt",
        filetype="txt",
        content="DAG Failure Details:\n"
        "DAG ID: {{ dag.dag_id }}\n"
        "Task ID: {{ task_instance.task_id }}\n"
        "Execution Date: {{ ds }}\n"
        "Run ID: {{ run_id }}\n"
        "\nPlease check Airflow UI for detailed logs and error messages.",
        title="DAG Failure Report - {{ ds }}",
        trigger_rule="one_failed",  # Only runs if upstream tasks fail
    )

    # Set up task dependencies
    latency_check_task >> csv_task >> send_csv_to_slack

    # Failure notification dependencies
    [latency_check_task, csv_task] >> notify_failure


# Create a separate DAG for ad-hoc dataset-specific checks
with DAG(
    dag_id=f"{DAG_ID}_dataset_specific",
    default_args=DEFAULT_ARGS,
    description="Ad-hoc BigQuery data latency monitoring for specific datasets with CSV export",
    schedule_interval=None,  # Manual trigger only
    tags=["data-quality", "monitoring", "alerts", "bigquery", "slack", "ad-hoc"],
    max_active_runs=3,
) as dataset_dag:

    # This DAG can be triggered manually with dataset parameter
    # Usage: airflow dags trigger data_latency_alerts_dataset_specific --conf '{"dataset_name": "your_dataset_prod_raw"}'
    dataset_check = BigQueryInsertJobOperator(
        task_id="run_dataset_specific_check",
        configuration={
            "query": {
                "query": "{% include '" + SQL_PATH + "' %}",
                "useLegacySql": False,
            }
        },
        params={
            "project_name": PROJECT_NAME,
            "audit_dataset_name": AUDIT_DATASET_NAME,
            "target_dataset": "{{ dag_run.conf.get('dataset_name', '') }}",
        },
        location=LOCATION,
        project_id=PROJECT_NAME,
        deferrable=True,
    )

    dataset_csv = convert_results_to_csv()

    dataset_send_csv = SlackAPIFileOperator(
        task_id="send_dataset_csv_to_slack",
        slack_conn_id=SLACK_CONN_ID,
        channels=SLACK_CHANNEL_ID,
        initial_comment="🎯 **Dataset-Specific Latency Check Results** 📊\n"
        "Dataset: `{{ dag_run.conf.get('dataset_name', 'Not specified') }}`\n"
        "Here are the latency monitoring results for the specific dataset.",
        filename="dataset_latency_check_{{ dag_run.conf.get('dataset_name', 'unknown') }}_{{ ds }}.csv",
        filetype="csv",
        content="{{ ti.xcom_pull(task_ids='convert_results_to_csv') }}",
        title="Dataset Latency Check Report - {{ ds }}",
    )

    dataset_failure = SlackAPIFileOperator(
        task_id="notify_dataset_failure",
        slack_conn_id=SLACK_CONN_ID,
        channels=SLACK_CHANNEL_ID,
        initial_comment="🚨 **Dataset-Specific Latency Check Failed** 🚨\n"
        f"📊 DAG: `{DAG_ID}_dataset_specific`\n"
        "🎯 Dataset: `{{ dag_run.conf.get('dataset_name', 'Not specified') }}`\n"
        "⏰ Execution Date: {{ ds }}\n"
        "❌ Check Airflow logs for details",
        filename="dataset_dag_failure_log_{{ ds }}.txt",
        filetype="txt",
        content="Dataset DAG Failure Details:\n"
        "DAG ID: {{ dag.dag_id }}\n"
        "Dataset: {{ dag_run.conf.get('dataset_name', 'Not specified') }}\n"
        "Task ID: {{ task_instance.task_id }}\n"
        "Execution Date: {{ ds }}\n"
        "Run ID: {{ run_id }}\n"
        "\nPlease check Airflow UI for detailed logs and error messages.",
        title="Dataset DAG Failure Report - {{ ds }}",
        trigger_rule="one_failed",
    )

    dataset_check >> dataset_csv >> dataset_send_csv
    [dataset_check, dataset_csv] >> dataset_failure
