"""
Data Latency Alerts DAG

This DAG orchestrates data latency monitoring by:
1. Executing BigQuery latency checks using native operators
2. Processing results and sending Slack notifications using native operators
3. Handling failures with native Slack notifications

The DAG is scheduled to run twice daily at 6 AM and 6 PM IST.
"""

import logging
from datetime import timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
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
SLACK_CHANNEL_ID = Variable.get("LATENCY_ALERTS__SLACK_CHANNEL_ID", "#data-alerts")
SLACK_SUCCESS_CHANNEL_ID = Variable.get("LATENCY_ALERTS__SLACK_SUCCESS_CHANNEL_ID", SLACK_CHANNEL_ID)
SLACK_FAILURE_CHANNEL_ID = Variable.get("LATENCY_ALERTS__SLACK_FAILURE_CHANNEL_ID", SLACK_CHANNEL_ID)

# SQL file path (relative to DAGs bucket)
SQL_PATH = "sql/latency_check_query.sql"


@task(task_id="process_results")
def process_results(**context) -> Dict[str, Any]:
    """
    Task to process the results from the BigQuery latency check and prepare summary.

    Returns:
        Dict containing processing summary for Slack notification
    """
    logging.info("Processing BigQuery latency check results")

    try:
        # Get results from the BigQuery task via XCom
        task_instance = context["task_instance"]
        results = task_instance.xcom_pull(task_ids="run_latency_check")

        if not results:
            logging.info("✅ No latency violations found - all tables are up to date")
            return {
                "status": "success",
                "total_violations": 0,
                "message": "✅ *Data Latency Check Completed Successfully*\n📊 No violations found - all tables are up to date",
                "channel": SLACK_SUCCESS_CHANNEL_ID,
            }
        else:
            # Process the results (list of dictionaries from BigQuery)
            total_violations = len(results)

            # Calculate statistics
            hours_list = [row.get("hours_since_last_update", 0) for row in results]
            max_hours = max(hours_list) if hours_list else 0
            avg_hours = sum(hours_list) / len(hours_list) if hours_list else 0

            # Group by dataset
            from collections import defaultdict

            dataset_stats = defaultdict(lambda: {"count": 0, "total_hours": 0})

            for row in results:
                dataset = row.get("dataset_id", "unknown")
                dataset_stats[dataset]["count"] += 1
                dataset_stats[dataset]["total_hours"] += row.get("hours_since_last_update", 0)

            # Calculate average delay for each dataset
            for stats_info in dataset_stats.values():
                stats_info["avg_hours"] = stats_info["total_hours"] / stats_info["count"]

            # Get top 5 datasets by average delay
            top_datasets = sorted(dataset_stats.items(), key=lambda x: x[1]["avg_hours"], reverse=True)[:5]

            logging.warning(f"⚠️ Found {total_violations} tables exceeding latency thresholds")
            logging.info(f"Max delay: {max_hours:.1f} hours")
            logging.info(f"Average delay: {avg_hours:.1f} hours")
            logging.info(f"Datasets affected: {len(dataset_stats)}")

            # Create Slack message for violations
            message = f"⚠️ *Data Latency Check Found Issues*\n"
            message += f"📊 DAG: `{context['dag'].dag_id}`\n"
            message += f"🔢 Total violations: {total_violations} tables\n"
            message += f"⏰ Max delay: {max_hours:.1f} hours\n"
            message += f"📈 Average delay: {avg_hours:.1f} hours\n"
            message += f"🗂️ Datasets affected: {len(dataset_stats)}\n\n"

            if top_datasets:
                message += "*Top affected datasets:*\n"
                for dataset, info in top_datasets[:3]:  # Top 3 for Slack readability
                    message += f"• `{dataset}`: {info['count']} tables (avg {info['avg_hours']:.1f}h delay)\n"

            return {
                "status": "warning",
                "total_violations": total_violations,
                "message": message,
                "channel": SLACK_SUCCESS_CHANNEL_ID,  # Still use success channel for warnings
                "max_hours": max_hours,
                "avg_hours": avg_hours,
                "datasets_affected": len(dataset_stats),
            }

    except Exception as e:
        error_msg = f"Error processing results: {str(e)}"
        logging.error(error_msg)

        return {
            "status": "error",
            "total_violations": 0,
            "message": f"❌ *Error Processing Latency Check Results*\n📊 DAG: `{context['dag'].dag_id}`\n🔍 Error: {error_msg}",
            "channel": SLACK_FAILURE_CHANNEL_ID,
        }


# Create the main DAG
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Native BigQuery and Slack operator-based data latency monitoring",
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

    # Task 2: Process the results
    process_task = process_results()

    # Task 3: Send Slack notification based on results
    notify_slack = SlackAPIPostOperator(
        task_id="notify_slack",
        slack_conn_id=SLACK_CONN_ID,
        channel="{{ ti.xcom_pull(task_ids='process_results')['channel'] }}",
        text="{{ ti.xcom_pull(task_ids='process_results')['message'] }}",
        username="Airflow Data Monitor",
    )

    # Task 4: Failure notification task (triggered on failures)
    notify_failure = SlackAPIPostOperator(
        task_id="notify_failure",
        slack_conn_id=SLACK_CONN_ID,
        channel=SLACK_FAILURE_CHANNEL_ID,
        text="""🚨 *Data Latency Check DAG Failed*
📊 DAG: `{{ dag.dag_id }}`
⏰ Execution Date: {{ ds }}
❌ Failed Task: {{ task_instance.task_id }}
🔍 Check Airflow logs for details""",
        username="Airflow Data Monitor",
        trigger_rule="one_failed",  # Only runs if upstream tasks fail
    )

    # Set up task dependencies
    latency_check_task >> process_task >> notify_slack

    # Failure notification dependencies
    [latency_check_task, process_task] >> notify_failure


# Create a separate DAG for ad-hoc dataset-specific checks
with DAG(
    dag_id=f"{DAG_ID}_dataset_specific",
    default_args=DEFAULT_ARGS,
    description="Ad-hoc BigQuery data latency monitoring for specific datasets",
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

    dataset_process = process_results()

    dataset_notify = SlackAPIPostOperator(
        task_id="notify_dataset_check",
        slack_conn_id=SLACK_CONN_ID,
        channel="{{ ti.xcom_pull(task_ids='process_results')['channel'] }}",
        text="""{{ ti.xcom_pull(task_ids='process_results')['message'] }}
🎯 *Dataset-Specific Check*: `{{ dag_run.conf.get('dataset_name', 'Not specified') }}`""",
        username="Airflow Data Monitor",
    )

    dataset_failure = SlackAPIPostOperator(
        task_id="notify_dataset_failure",
        slack_conn_id=SLACK_CONN_ID,
        channel=SLACK_FAILURE_CHANNEL_ID,
        text="""🚨 *Dataset-Specific Latency Check Failed*
📊 DAG: `{{ dag.dag_id }}`
🎯 Dataset: `{{ dag_run.conf.get('dataset_name', 'Not specified') }}`
⏰ Execution Date: {{ ds }}
❌ Failed Task: {{ task_instance.task_id }}""",
        username="Airflow Data Monitor",
        trigger_rule="one_failed",
    )

    dataset_check >> dataset_process >> dataset_notify
    [dataset_check, dataset_process] >> dataset_failure
