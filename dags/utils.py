"""
Utility functions for data latency alerts DAG.

This module provides simple functions for:
- BigQuery operations and latency checks
- Slack messaging and file uploads
- CSV data processing
"""

import csv
import io
import json
import logging
import os
from typing import Any, Dict, List, Union
from urllib.parse import quote

from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.slack.hooks.slack import SlackHook
from jinja2 import Template


def load_slack_blocks() -> Dict[str, Any]:
    """
    Load Slack block templates from JSON file.

    Returns:
        Dictionary containing block templates
    """
    # Get the current directory where this utils.py file is located
    current_dir = os.path.dirname(os.path.abspath(__file__))
    blocks_file_path = os.path.join(current_dir, "slack_blocks.json")

    try:
        with open(blocks_file_path, "r") as file:
            return json.load(file)
    except Exception as e:
        logging.error(f"Failed to load Slack blocks from {blocks_file_path}: {e}")
        return {}


def build_airflow_urls(task_instance=None, dag_id: str = None) -> Dict[str, str]:
    """
    Build Airflow UI URLs using task_instance.log_url when available.

    Args:
        task_instance: Airflow TaskInstance object (preferred)
        dag_id: DAG identifier (fallback if task_instance not available)

    Returns:
        Dictionary with airflow URLs
    """
    urls = {}

    if task_instance:
        # Use the actual log URL from task instance - this is the most reliable
        if hasattr(task_instance, "log_url") and task_instance.log_url:
            urls["airflow_task_url"] = task_instance.log_url

        # Build DAG URL from task instance info
        if hasattr(task_instance, "dag_id"):
            try:
                airflow_base_url = Variable.get("AIRFLOW_BASE_URL", "http://localhost:8080")
            except Exception:
                airflow_base_url = "http://localhost:8080"

            airflow_base_url = airflow_base_url.rstrip("/")
            encoded_dag_id = quote(task_instance.dag_id)
            urls["airflow_dag_url"] = f"{airflow_base_url}/dags/{encoded_dag_id}/grid"
            urls["airflow_url"] = urls["airflow_dag_url"]

    elif dag_id:
        # Fallback to manual URL construction if no task_instance
        try:
            airflow_base_url = Variable.get("AIRFLOW_BASE_URL", "http://localhost:8080")
        except Exception:
            airflow_base_url = "http://localhost:8080"

        airflow_base_url = airflow_base_url.rstrip("/")
        encoded_dag_id = quote(dag_id)
        urls["airflow_dag_url"] = f"{airflow_base_url}/dags/{encoded_dag_id}/grid"
        urls["airflow_url"] = urls["airflow_dag_url"]

    return urls


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
    try:
        logging.info("🔍 Starting BigQuery latency check...")
        logging.info(f"📊 Project: {project_id}, Location: {location}, Connection: {gcp_conn_id}")

        # Initialize BigQuery hook
        bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, location=location, use_legacy_sql=False)

        # Format the query with parameters using Jinja2 templating
        if params:
            template = Template(sql_query)
            formatted_query = template.render(params=params)
        else:
            formatted_query = sql_query

        logging.info(f"📋 Parameters: {params}")
        logging.info(f"📝 Formatted query (first 500 chars): {formatted_query[:500]}...")

        # Execute the query
        logging.info("⚡ Executing BigQuery...")
        results = bq_hook.get_pandas_df(sql=formatted_query, parameters=None, dialect="standard")

        # Convert DataFrame to list of dictionaries
        results_list = results.to_dict("records") if not results.empty else []

        logging.info(f"✅ BigQuery execution completed. Found {len(results_list)} latency violations")

        if results_list:
            logging.info(f"📈 Sample result keys: {list(results_list[0].keys()) if results_list else 'None'}")

        return results_list

    except Exception as e:
        error_msg = f"❌ BigQuery execution failed: {str(e)}"
        logging.error(error_msg)
        logging.error(f"📋 Query parameters were: {params}")
        logging.error(
            f"📝 Query was: {formatted_query[:1000] if 'formatted_query' in locals() else 'Query formatting failed'}"
        )
        raise Exception(error_msg)


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

        # Use the correct SlackHook API method for file upload
        response = slack_hook.call(
            api_method="files.upload",
            data={
                "channels": channel,
                "content": file_content,
                "filename": filename,
                "initial_comment": initial_comment,
                "filetype": filetype,
            },
        )

        if response.get("ok"):
            logging.info(f"✅ File sent to {channel} successfully")
            results.append({"channel": channel, "status": "success"})
        else:
            logging.error(f"❌ Failed to send file to {channel}: {response}")
            results.append({"channel": channel, "status": "error", "error": response})

    return {"results": results}


def send_slack_message_with_blocks(
    channels: Union[str, List[str]],
    blocks: List[Dict[str, Any]],
    fallback_text: str = "",
    slack_conn_id: str = "slack_default",
) -> Dict[str, Any]:
    """
    Send a Slack message with rich blocks formatting.

    Args:
        channels: Slack channel ID/name or list of channels
        blocks: Slack blocks to send
        fallback_text: Fallback text for notifications
        slack_conn_id: Airflow Slack connection ID

    Returns:
        Dictionary with send results
    """
    # Ensure channels is a list
    if isinstance(channels, str):
        channels = [channels]

    logging.info(f"📤 Sending blocks message to Slack channels: {channels}")

    slack_hook = SlackHook(slack_conn_id=slack_conn_id)

    # Send to each channel
    results = []
    for channel in channels:
        logging.info(f"Sending to channel: {channel}")

        # Use the correct SlackHook API method with blocks
        response = slack_hook.call(
            api_method="chat.postMessage",
            json={"channel": channel, "blocks": blocks, "text": fallback_text},  # Fallback for notifications
        )

        if response.get("ok"):
            logging.info(f"✅ Blocks message sent to {channel} successfully")
            results.append({"channel": channel, "status": "success"})
        else:
            logging.error(f"❌ Failed to send blocks message to {channel}: {response}")
            results.append({"channel": channel, "status": "error", "error": response})

    return {"results": results}


def send_latency_report_to_slack(
    csv_content: str,
    channels: Union[str, List[str]],
    execution_date: str,
    dag_id: str = "data_latency_alerts",
    task_instance=None,
    slack_conn_id: str = "slack_default",
) -> Dict[str, Any]:
    """
    Send latency check report to Slack channel(s) with rich blocks formatting.

    Args:
        csv_content: CSV content to send
        channels: Slack channel(s) to send to
        execution_date: DAG execution date
        dag_id: DAG identifier
        task_instance: Optional TaskInstance for log URLs
        slack_conn_id: Airflow Slack connection ID

    Returns:
        Dictionary with send results
    """
    # Load Slack block templates
    block_templates = load_slack_blocks()

    # Count violations from CSV content
    violations_count = 0
    if csv_content and "No latency violations found" not in csv_content:
        # Count lines minus header
        violations_count = len(csv_content.strip().split("\n")) - 1
        violations_count = max(0, violations_count)  # Ensure non-negative

    # Build Airflow URLs
    urls = build_airflow_urls(task_instance=task_instance, dag_id=dag_id)

    # Choose appropriate block template
    if violations_count > 0:
        template_key = "latency_report_success"
    else:
        template_key = "latency_report_no_violations"

    # Get the block template
    if template_key in block_templates:
        blocks = block_templates[template_key]["blocks"]

        # Replace placeholders in blocks using string replacement
        blocks_str = json.dumps(blocks)

        # Replace all placeholders
        replacements = {
            "{execution_date}": execution_date,
            "{dag_id}": dag_id,
            "{violations_count}": str(violations_count),
        }
        replacements.update({f"{{{k}}}": v for k, v in urls.items()})

        for placeholder, value in replacements.items():
            blocks_str = blocks_str.replace(placeholder, value)

        formatted_blocks = json.loads(blocks_str)

        # Send blocks message first
        blocks_result = send_slack_message_with_blocks(
            channels=channels,
            blocks=formatted_blocks,
            fallback_text=f"Data Latency Check Results - {execution_date}",
            slack_conn_id=slack_conn_id,
        )

        # If there are violations, also send the CSV file
        if violations_count > 0:
            filename = f"data_latency_report_{execution_date}.csv"
            file_result = send_slack_file(
                channels=channels,
                file_content=csv_content,
                filename=filename,
                initial_comment="",  # No comment needed, blocks message provides context
                filetype="csv",
                slack_conn_id=slack_conn_id,
            )
            return {"blocks_result": blocks_result, "file_result": file_result}

        return {"blocks_result": blocks_result}

    else:
        # Fallback to simple message if blocks not available
        logging.warning(f"Block template '{template_key}' not found, using fallback message")
        simple_message = f"Data Latency Check Results - {execution_date}: {violations_count} violations found"

        return send_slack_message_with_blocks(
            channels=channels,
            blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": simple_message}}],
            fallback_text=simple_message,
            slack_conn_id=slack_conn_id,
        )


def send_failure_notification(
    error_message: str,
    channels: Union[str, List[str]],
    dag_id: str,
    execution_date: str,
    failed_task_id: str = None,
    task_instance=None,
    slack_conn_id: str = "slack_default",
) -> Dict[str, Any]:
    """
    Send failure notification to Slack channel(s) with rich blocks formatting.

    Args:
        error_message: The error message to send
        channels: Slack channel(s) to send to
        dag_id: DAG identifier
        execution_date: DAG execution date
        failed_task_id: Optional task ID that failed
        task_instance: Optional TaskInstance for log URLs
        slack_conn_id: Airflow Slack connection ID

    Returns:
        Dictionary with send results
    """
    # Load Slack block templates
    block_templates = load_slack_blocks()

    # Build Airflow URLs
    urls = build_airflow_urls(task_instance=task_instance, dag_id=dag_id)

    # Determine failure type based on error message and choose appropriate template
    template_key = "dag_failure"  # Default template

    if "bigquery" in error_message.lower() or "BigQuery" in error_message:
        template_key = "bigquery_failure"
        failed_task_id = failed_task_id or "run_latency_check"
    elif "slack" in error_message.lower() or "Slack" in error_message:
        template_key = "slack_failure"
        failed_task_id = failed_task_id or "convert_and_send_to_slack"

    # Get the block template
    if template_key in block_templates:
        blocks = block_templates[template_key]["blocks"]

        # Replace placeholders in blocks using string replacement
        blocks_str = json.dumps(blocks)

        # Replace all placeholders
        replacements = {
            "{execution_date}": execution_date,
            "{dag_id}": dag_id,
            "{failed_task_id}": failed_task_id or "unknown",
            "{error_message}": error_message[:500],  # Limit error message length
        }
        replacements.update({f"{{{k}}}": v for k, v in urls.items()})

        for placeholder, value in replacements.items():
            blocks_str = blocks_str.replace(placeholder, value)

        formatted_blocks = json.loads(blocks_str)

        # Send blocks message
        return send_slack_message_with_blocks(
            channels=channels,
            blocks=formatted_blocks,
            fallback_text=f"Data Latency Check Failed - {dag_id} - {execution_date}",
            slack_conn_id=slack_conn_id,
        )

    else:
        # Fallback to simple message if blocks not available
        logging.warning(f"Block template '{template_key}' not found, using fallback message")
        simple_message = f"DAG {dag_id} failed on {execution_date}: {error_message}"

        return send_slack_message_with_blocks(
            channels=channels,
            blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": simple_message}}],
            fallback_text=simple_message,
            slack_conn_id=slack_conn_id,
        )
