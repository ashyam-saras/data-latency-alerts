"""
Utility functions for data latency alerts DAG.

This module provides simple functions for:
- BigQuery operations and latency checks
- Slack messaging and file uploads
- CSV data processing
"""

import csv
import io
import logging
from typing import Any, Dict, List, Union

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.slack.hooks.slack import SlackHook


def execute_bigquery_latency_check(
    sql_query: str,
    project_id: str,
    location: str = "us-central1",
    gcp_conn_id: str = "google_cloud_default",
    **params,
) -> List[Dict[str, Any]]:
    """
    Execute BigQuery latency check query and return results.

    Args:
        sql_query: The SQL query to execute
        project_id: GCP project ID
        location: BigQuery location
        gcp_conn_id: Airflow connection ID for BigQuery
        **params: Additional parameters to pass to the query

    Returns:
        List of dictionaries containing query results
    """
    logging.info("🔍 Starting BigQuery latency check...")

    # Initialize BigQuery hook
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, location=location, use_legacy_sql=False)

    # Format the query with parameters
    formatted_query = sql_query.format(**params) if params else sql_query

    logging.info(f"📊 Executing query in project: {project_id}")

    # Execute the query
    results = bq_hook.get_pandas_df(sql=formatted_query, parameters=None, dialect="standard")

    # Convert DataFrame to list of dictionaries
    results_list = results.to_dict("records") if not results.empty else []

    logging.info(f"✅ BigQuery execution completed. Found {len(results_list)} latency violations")

    return results_list


def convert_results_to_csv(results: List[Dict[str, Any]]) -> str:
    """
    Convert BigQuery results to CSV format.

    Args:
        results: List of dictionaries from BigQuery

    Returns:
        CSV content as string
    """
    if not results:
        # Create CSV with header and no violations message
        csv_content = "message\nNo latency violations found - all tables are up to date! ✅"
        logging.info("✅ No latency violations found")
        return csv_content

    # Convert results to CSV format
    output = io.StringIO()

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


def send_slack_file(
    channels: Union[str, List[str]],
    file_content: str,
    filename: str,
    initial_comment: str = "",
    filetype: str = "csv",
    slack_conn_id: str = "slack_default",
) -> Dict[str, Any]:
    """
    Send a file to Slack channel(s).

    Args:
        channels: Slack channel ID/name or list of channels
        file_content: Content of the file to send
        filename: Name of the file
        initial_comment: Comment to include with the file
        filetype: Type of file (csv, txt, etc.)
        slack_conn_id: Airflow Slack connection ID

    Returns:
        Dictionary with send results
    """
    # Ensure channels is a list
    if isinstance(channels, str):
        channels = [channels]

    logging.info(f"📤 Sending file '{filename}' to Slack channels: {channels}")

    slack_hook = SlackHook(slack_conn_id=slack_conn_id)

    # Send to each channel
    results = []
    for channel in channels:
        logging.info(f"Sending to channel: {channel}")

        response = slack_hook.send_file(
            channels=channel, file=file_content, filename=filename, initial_comment=initial_comment, filetype=filetype
        )

        if response.get("ok"):
            logging.info(f"✅ File sent to {channel} successfully")
            results.append({"channel": channel, "status": "success"})
        else:
            logging.error(f"❌ Failed to send file to {channel}: {response}")
            results.append({"channel": channel, "status": "error", "error": response})

    return {"results": results}


def send_slack_message(
    channels: Union[str, List[str]],
    text: str,
    slack_conn_id: str = "slack_default",
) -> Dict[str, Any]:
    """
    Send a text message to Slack channel(s).

    Args:
        channels: Slack channel ID/name or list of channels
        text: Message text to send
        slack_conn_id: Airflow Slack connection ID

    Returns:
        Dictionary with send results
    """
    # Ensure channels is a list
    if isinstance(channels, str):
        channels = [channels]

    logging.info(f"📤 Sending message to Slack channels: {channels}")

    slack_hook = SlackHook(slack_conn_id=slack_conn_id)

    # Send to each channel
    results = []
    for channel in channels:
        logging.info(f"Sending to channel: {channel}")

        response = slack_hook.send_message(channel=channel, text=text)

        if response.get("ok"):
            logging.info(f"✅ Message sent to {channel} successfully")
            results.append({"channel": channel, "status": "success"})
        else:
            logging.error(f"❌ Failed to send message to {channel}: {response}")
            results.append({"channel": channel, "status": "error", "error": response})

    return {"results": results}


def send_latency_report_to_slack(
    csv_content: str, channels: Union[str, List[str]], execution_date: str, slack_conn_id: str = "slack_default"
) -> Dict[str, Any]:
    """
    Send latency check report to Slack channel(s).

    Args:
        csv_content: CSV content to send
        channels: Slack channel(s) to send to
        execution_date: DAG execution date
        slack_conn_id: Airflow Slack connection ID

    Returns:
        Dictionary with send results
    """
    initial_comment = f"""🔍 **Data Latency Check Results** 📊

Here are the latest latency monitoring results for {execution_date}.
Please find the detailed report in the attached CSV file."""

    filename = f"data_latency_report_{execution_date}.csv"

    return send_slack_file(
        channels=channels,
        file_content=csv_content,
        filename=filename,
        initial_comment=initial_comment,
        filetype="csv",
        slack_conn_id=slack_conn_id,
    )


def send_failure_notification(
    error_message: str,
    channels: Union[str, List[str]],
    dag_id: str,
    execution_date: str,
    slack_conn_id: str = "slack_default",
) -> Dict[str, Any]:
    """
    Send failure notification to Slack channel(s).

    Args:
        error_message: The error message to send
        channels: Slack channel(s) to send to
        dag_id: DAG identifier
        execution_date: DAG execution date
        slack_conn_id: Airflow Slack connection ID

    Returns:
        Dictionary with send results
    """
    failure_message = f"""🚨 **Data Latency Check DAG Failed** 🚨

📊 DAG: `{dag_id}`
⏰ Execution Date: {execution_date}
❌ Error: {error_message}

🔗 Please check the Airflow UI for detailed logs."""

    return send_slack_message(channels=channels, text=failure_message, slack_conn_id=slack_conn_id)
