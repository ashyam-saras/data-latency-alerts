"""
Utility functions for data latency alerts DAG.

This module provides simple functions for:
- BigQuery operations and latency checks
- Slack messaging and file uploads
- CSV data processing
"""

import io
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import quote

import pandas as pd
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.slack.hooks.slack import SlackHook
from jinja2 import Template

# Slack routing edge cases (documented per requirements):
# - Duplicate regex patterns
# - Overlapping patterns
# - Dataset present in multiple patterns
# - Missing default channel mapping
# - Patterns with empty channel values
# - Patterns that match empty strings
# - JSON values that are null or not strings
# - Trailing commas or malformed JSON


def _normalize_channel_value(raw_value: Any, *, context: str) -> List[str]:
    """
    Convert a channel value into a list of channel strings.
    """
    if raw_value is None:
        raise ValueError(f"Channel value for {context} cannot be null")
    if not isinstance(raw_value, str):
        raise ValueError(
            f"Channel value for {context} must be a string, got {type(raw_value).__name__}"
        )

    channels = [channel.strip() for channel in raw_value.split(",") if channel.strip()]
    if not channels:
        raise ValueError(f"No valid Slack channels found for {context}")
    return channels


def _deduplicate_preserve_order(items: List[str]) -> List[str]:
    """
    Remove duplicates from a list while preserving order.
    """
    seen = set()
    deduped = []
    for item in items:
        if item not in seen:
            deduped.append(item)
            seen.add(item)
    return deduped


def _extract_dataset_identifier(record: Dict[str, Any]) -> Optional[str]:
    """
    Extract a dataset identifier from a result record.
    """
    candidate_keys = ("dataset_id", "dataset", "dataset_name", "table_schema")
    for key in candidate_keys:
        value = record.get(key)
        if value is not None:
            return str(value)
    return None

def parse_slack_channels_config(config_str: str) -> Dict[str, Any]:
    """
    Parse Slack channels configuration from Airflow Variable.

    Expected format (JSON string):
    {
      "3461": "#clientA-alerts",
      "4321|3476": "#clientAB-alerts",
      "default": "#data-alerts,#monitoring"
    }

    Rules:
        - JSON must be a dictionary
        - A "default" key is mandatory and must contain at least one channel
        - Each non-default key is treated as a regex pattern mapped to comma-separated channels

    Returns:
        dict with the following keys:
            - default: primary default channel (first in list)
            - channels: list of default channels
            - default_channels: alias for channels (explicit naming)
            - patterns: {pattern: [channels]}
            - pattern_entries: [{"pattern": str, "regex": Pattern, "channels": [str]}]
            - is_legacy: always False (maintains backwards-compatible flag)

    Raises:
        ValueError: for malformed or incomplete configurations
    """
    if not config_str or not config_str.strip():
        raise ValueError("Slack channels configuration cannot be empty. Please provide a valid configuration.")

    try:
        config_json = json.loads(config_str.strip())
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"LATENCY_ALERTS__SLACK_CHANNELS must contain valid JSON: {exc.msg} (pos {exc.pos})"
        ) from exc

    if not isinstance(config_json, dict):
        raise ValueError(
            f"Slack channel configuration must be a JSON object, got {type(config_json).__name__}"
        )

    if "default" not in config_json:
        raise ValueError("Slack channel configuration must include a 'default' key with channel(s).")

    default_channels = _normalize_channel_value(config_json["default"], context="'default'")
    primary_default = default_channels[0]

    pattern_entries: List[Dict[str, Any]] = []
    pattern_map: Dict[str, List[str]] = {}

    for key, value in config_json.items():
        if key == "default":
            continue

        pattern_str = str(key).strip()
        if not pattern_str:
            raise ValueError("Pattern keys cannot be empty strings.")

        channels = _normalize_channel_value(value, context=f"pattern '{pattern_str}'")

        try:
            compiled_regex = re.compile(pattern_str)
        except re.error as exc:
            raise ValueError(f"Invalid regex pattern '{pattern_str}': {exc}") from exc

        pattern_entries.append(
            {"pattern": pattern_str, "regex": compiled_regex, "channels": channels}
        )
        pattern_map[pattern_str] = channels

    logging.info(
        "Parsed Slack channel mapping with %d pattern(s) and %d default channel(s).",
        len(pattern_entries),
        len(default_channels),
    )

    return {
        "default": primary_default,
        "channels": default_channels,
        "default_channels": default_channels,
        "patterns": pattern_map,
        "pattern_entries": pattern_entries,
        "is_legacy": False,
    }
def resolve_channels_for_results(
    results: List[Dict[str, Any]],
    channel_config: Dict[str, Any],
    fallback_channels: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Resolve dataset-specific routing rules.

    Args:
        results: List of BigQuery result rows.
        channel_config: Output of parse_slack_channels_config.
        fallback_channels: Optional static fallback (legacy compatibility).

    Returns:
        List of routing entries, each containing:
            {
                "channels": [list of Slack channels],
                "results": [records scoped to those channels]
            }
    """
    if not isinstance(channel_config, dict):
        if fallback_channels:
            return [{"channels": fallback_channels, "results": results}]
        raise ValueError("Channel configuration must be a dictionary.")

    default_channels = (
        channel_config.get("default_channels")
        or channel_config.get("channels")
        or fallback_channels
    )
    if not default_channels:
        raise ValueError("Slack channel configuration must define default channels.")

    pattern_entries: List[Dict[str, Any]] = channel_config.get("pattern_entries", [])

    if not results:
        return [{"channels": default_channels, "results": []}]

    channel_routes: Dict[Tuple[str, ...], List[Dict[str, Any]]] = {}

    for record in results:
        dataset_identifier = _extract_dataset_identifier(record)

        if not dataset_identifier:
            continue

        for entry in pattern_entries:
            if entry["regex"].search(dataset_identifier):
                matched_channels = _deduplicate_preserve_order(entry["channels"])
                route_key = tuple(matched_channels)
                channel_routes.setdefault(route_key, []).append(record)

    routes = [{"channels": list(default_channels), "results": results}]

    for channels, routed_results in channel_routes.items():
        routes.append({"channels": list(channels), "results": routed_results})

    logging.info("Prepared %d Slack routing bundle(s).", len(routes))
    return routes


def get_failure_channels(
    channel_config: Dict[str, Any],
    fallback_channels: Optional[List[str]] = None
) -> List[str]:
    """
    Get channels for failure notifications.
    
    For failures, we always use the default channel(s) as they indicate
    system-level issues rather than dataset-specific problems.
    
    Includes error handling with fallback to ensure critical failure alerts are never lost.
    
    Args:
        channel_config: Parsed channel configuration dictionary
        fallback_channels: Optional fallback channels if configuration is invalid
        
    Returns:
        List of Slack channel IDs for failure notifications
        
    Risk Mitigation:
        - Always returns at least one channel (fallback if needed)
        - Handles configuration errors gracefully
    """
    if not channel_config or not isinstance(channel_config, dict):
        logging.warning("⚠️ Invalid channel configuration for failure notification, using fallback")
        return fallback_channels or ["C065MG2L63U"]

    channels = (
        channel_config.get("default_channels")
        or channel_config.get("channels")
        or ([channel_config.get("default")] if channel_config.get("default") else None)
    )

    if not channels:
        logging.warning("⚠️ No default channels found, using fallback for failures")
        return fallback_channels or ["C065MG2L63U"]

    logging.info("📢 Using failure notification channel(s): %s", channels)
    return channels



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
                airflow_base_url = Variable.get("LATENCY_ALERTS__AIRFLOW_BASE_URL", "http://localhost:8080")
            except Exception:
                airflow_base_url = "http://localhost:8080"

            airflow_base_url = airflow_base_url.rstrip("/")
            encoded_dag_id = quote(task_instance.dag_id)
            urls["airflow_dag_url"] = f"{airflow_base_url}/dags/{encoded_dag_id}/grid"
            urls["airflow_url"] = urls["airflow_dag_url"]

    elif dag_id:
        # Fallback to manual URL construction if no task_instance
        try:
            airflow_base_url = Variable.get("LATENCY_ALERTS__AIRFLOW_BASE_URL", "http://localhost:8080")
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

        # Convert pandas Timestamp objects to strings for JSON serialization
        if results_list:
            for record in results_list:
                for key, value in record.items():
                    # Convert pandas Timestamp objects to ISO format strings
                    if hasattr(value, "isoformat"):  # This catches pandas Timestamps
                        record[key] = value.isoformat()
                    # Handle NaT (Not a Time) values
                    elif str(value) == "NaT":
                        record[key] = None

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


def calculate_dataset_summary(results: List[Dict[str, Any]]) -> Tuple[str, Dict[str, int]]:
    """
    Calculate summary statistics by dataset.

    Args:
        results: List of dictionaries from BigQuery

    Returns:
        Tuple of (summary_text, dataset_counts)
    """
    if not results:
        return "✅ No latency violations found - all tables are up to date!", {}

    # Count violations by dataset (table_schema)
    dataset_counts = {}
    for record in results:
        dataset = record.get("table_schema", "Unknown")
        dataset_counts[dataset] = dataset_counts.get(dataset, 0) + 1

    # Create summary text
    total_violations = len(results)
    total_datasets = len(dataset_counts)

    summary_lines = [f"Found {total_violations} table violations across {total_datasets} datasets:"]

    # Sort datasets by violation count (descending)
    sorted_datasets = sorted(dataset_counts.items(), key=lambda x: x[1], reverse=True)

    for dataset, count in sorted_datasets:
        summary_lines.append(f"• *{dataset}*: {count} tables violate threshold")

    summary_text = "\n".join(summary_lines)

    logging.info(f"📊 Dataset summary: {total_violations} violations across {total_datasets} datasets")
    for dataset, count in sorted_datasets:
        logging.info(f"  - {dataset}: {count} tables")

    return summary_text, dataset_counts


def convert_results_to_xlsx(results: List[Dict[str, Any]]) -> bytes:
    """
    Convert BigQuery results to XLSX format.

    Args:
        results: List of dictionaries from BigQuery

    Returns:
        XLSX content as bytes
    """
    if not results:
        # Create DataFrame with no violations message
        df = pd.DataFrame({"message": ["No latency violations found - all tables are up to date! ✅"]})
        logging.info("✅ No latency violations found")
    else:
        # Convert results to DataFrame
        df = pd.DataFrame(results)
        total_violations = len(results)
        logging.info(f"📊 Generated XLSX with {total_violations} latency violations")

    # Convert DataFrame to XLSX bytes
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Latency Violations", index=False)

        # Auto-adjust column widths
        worksheet = writer.sheets["Latency Violations"]
        for col in worksheet.columns:
            max_length = 0
            column = col[0].column_letter
            for cell in col:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
            worksheet.column_dimensions[column].width = adjusted_width

    xlsx_content = output.getvalue()
    output.close()

    return xlsx_content


def send_slack_file(
    channels: Union[str, List[str]],
    file_content: Union[str, bytes],
    filename: str,
    initial_comment: str = "",
    filetype: str = "csv",
    slack_conn_id: str = "slack_default",
    thread_ts: Union[str, Dict[str, str], None] = None,
) -> Dict[str, Any]:
    """
    Send a file to Slack channel(s).

    Args:
        channels: Slack channel ID/name or list of channels
        file_content: Content of the file to send (string for text files, bytes for binary files)
        filename: Name of the file
        initial_comment: Comment to include with the file
        filetype: Type of file (csv, txt, xlsx, etc.)
        slack_conn_id: Airflow Slack connection ID
        thread_ts: Optional message timestamp to reply in thread. Can be:
                   - String: timestamp for all channels
                   - Dict: {channel: timestamp} mapping
                   - None: send as regular message (not threaded)

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

        # Determine thread timestamp for this channel
        channel_thread_ts = None
        if thread_ts:
            if isinstance(thread_ts, str):
                # Single timestamp for all channels
                channel_thread_ts = thread_ts
            elif isinstance(thread_ts, dict):
                # Channel-specific timestamp mapping
                channel_thread_ts = thread_ts.get(channel)

        if channel_thread_ts:
            logging.info(f"Sending file as threaded reply to message {channel_thread_ts}")

        # Use different approach for binary vs text files
        if isinstance(file_content, bytes):
            # Binary file upload (XLSX, etc.) - use SlackHook client directly
            import io

            try:
                # Use the Slack client directly for binary file uploads
                response = slack_hook.client.files_upload_v2(
                    channels=channel,
                    file=io.BytesIO(file_content),
                    filename=filename,
                    initial_comment=initial_comment,
                    filetype=filetype,
                    thread_ts=channel_thread_ts,  # Add thread support
                )
            except Exception as e:
                logging.error(f"❌ Failed to upload binary file using client method: {e}")
                # Fallback: Convert to base64 and try with regular API
                import base64

                file_content_b64 = base64.b64encode(file_content).decode("utf-8")
                upload_data = {
                    "channels": channel,
                    "content": file_content_b64,
                    "filename": filename,
                    "initial_comment": initial_comment,
                    "filetype": filetype,
                }
                if channel_thread_ts:
                    upload_data["thread_ts"] = channel_thread_ts

                response = slack_hook.call(
                    api_method="files.upload",
                    data=upload_data,
                )
        else:
            # Text file upload (CSV, TXT, etc.)
            upload_data = {
                "channels": channel,
                "content": file_content,
                "filename": filename,
                "initial_comment": initial_comment,
                "filetype": filetype,
            }
            if channel_thread_ts:
                upload_data["thread_ts"] = channel_thread_ts

            response = slack_hook.call(
                api_method="files.upload",
                data=upload_data,
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
        Dictionary with send results including message timestamps for each channel
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
            # Include the message timestamp for threading
            message_ts = response.get("ts")
            results.append({"channel": channel, "status": "success", "ts": message_ts, "response": response})
        else:
            logging.error(f"❌ Failed to send blocks message to {channel}: {response}")
            results.append({"channel": channel, "status": "error", "error": response})

    return {"results": results}


def send_latency_report_to_slack(
    xlsx_content: bytes,
    results: List[Dict[str, Any]],
    channels: Union[str, List[str], dict, List[Dict[str, Any]]],
    execution_date: str,
    dag_id: str = "data_latency_alerts",
    task_instance=None,
    slack_conn_id: str = "slack_default",
) -> Dict[str, Any]:
    """
    Send latency check report to Slack channel(s) with rich blocks formatting.

    Args:
        xlsx_content: XLSX content bytes to send
        results: List of violation results for summary calculation
        channels: Slack channel(s) to send to
        execution_date: DAG execution date
        dag_id: DAG identifier
        task_instance: Optional TaskInstance for log URLs
        slack_conn_id: Airflow Slack connection ID

    Returns:
        Dictionary with send results
    """
    block_templates = load_slack_blocks()
    urls = build_airflow_urls(task_instance=task_instance, dag_id=dag_id)

    def _send_to_channels(
        target_channels: Union[str, List[str]],
        target_results: List[Dict[str, Any]],
        xlsx_override: Optional[bytes],
    ) -> Dict[str, Any]:
        summary_text, _ = calculate_dataset_summary(target_results)
        violations_count = len(target_results)
        template_key = "latency_report_success" if violations_count > 0 else "latency_report_no_violations"

        if template_key not in block_templates:
            logging.warning("Block template '%s' not found, sending fallback text.", template_key)
            simple_message = f"Data Latency Check Results - {execution_date}: {violations_count} violations found"
            return send_slack_message_with_blocks(
                channels=target_channels,
                blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": simple_message}}],
                fallback_text=simple_message,
                slack_conn_id=slack_conn_id,
            )

        blocks = block_templates[template_key]["blocks"]
        blocks_str = json.dumps(blocks)

        replacements = {
            "{execution_date}": execution_date,
            "{dag_id}": dag_id,
            "{violations_count}": str(violations_count),
            "{summary_text}": summary_text.replace('"', '\\"').replace("\n", "\\n"),
        }
        for key, value in urls.items():
            replacements[f"{{{key}}}"] = value.replace('"', '\\"') if value else ""

        for placeholder, value in replacements.items():
            blocks_str = blocks_str.replace(placeholder, value)

        try:
            formatted_blocks = json.loads(blocks_str)
        except json.JSONDecodeError as exc:
            logging.error("❌ JSON parsing failed at position %s (%s)", exc.pos, exc.msg)
            raise

        blocks_result = send_slack_message_with_blocks(
            channels=target_channels,
            blocks=formatted_blocks,
            fallback_text=f"Data Latency Check Results - {execution_date}",
            slack_conn_id=slack_conn_id,
        )

        if violations_count == 0:
            return {"blocks_result": blocks_result}

        thread_timestamps = {
            entry["channel"]: entry["ts"]
            for entry in blocks_result.get("results", [])
            if entry.get("status") == "success" and entry.get("ts")
        }

        if thread_timestamps:
            logging.info("📎 Sending files as threaded replies for channels: %s", list(thread_timestamps.keys()))

        file_bytes = xlsx_override or convert_results_to_xlsx(target_results)
        filename = f"data_latency_report_{execution_date}.xlsx"
        file_result = send_slack_file(
            channels=target_channels,
            file_content=file_bytes,
            filename=filename,
            initial_comment="",
            filetype="xlsx",
            slack_conn_id=slack_conn_id,
            thread_ts=thread_timestamps if thread_timestamps else None,
        )
        return {"blocks_result": blocks_result, "file_result": file_result}

    def _looks_like_routing_payload(payload: Any) -> bool:
        return isinstance(payload, list) and payload and all(
            isinstance(entry, dict) and "channels" in entry for entry in payload
        )

    if _looks_like_routing_payload(channels):
        route_payload: List[Dict[str, Any]] = channels  # type: ignore[assignment]
        responses = []
        for route in route_payload:
            target_channels = route.get("channels") or []
            if not target_channels:
                logging.warning("Skipping route with empty channel list: %s", route)
                continue
            route_results = route.get("results", [])
            responses.append(
                {
                    "channels": target_channels,
                    "response": _send_to_channels(target_channels, route_results, None),
                }
            )
        return {"routes": responses}

    if isinstance(channels, dict):
        responses = []
        for channel, scoped_results in channels.items():
            responses.append(
                {
                    "channels": [channel],
                    "response": _send_to_channels([channel], scoped_results, None),
                }
            )
        return {"routes": responses}

    return _send_to_channels(channels, results, xlsx_content)


def send_failure_notification(
    error_message: str,
    channels: Union[str, List[str],dict],
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

        # Convert blocks to string for placeholder replacement
        blocks_str = json.dumps(blocks)

        # Replace placeholders with raw values (not JSON-encoded)
        # This avoids double-encoding issues since we're inserting into JSON strings
        replacements = {
            "{execution_date}": execution_date,
            "{dag_id}": dag_id,
            "{failed_task_id}": failed_task_id or "unknown",
            "{error_message}": error_message[:500].replace('"', '\\"').replace("\n", "\\n"),  # Escape for JSON
        }
        # URLs need escaping too
        for k, v in urls.items():
            replacements[f"{{{k}}}"] = v.replace('"', '\\"') if v else ""

        for placeholder, value in replacements.items():
            blocks_str = blocks_str.replace(placeholder, value)

        # Parse back to blocks object
        try:
            formatted_blocks = json.loads(blocks_str)
        except json.JSONDecodeError as e:
            logging.error(f"❌ [FAILURE] JSON parsing failed at position {e.pos}")
            logging.error(f"❌ [FAILURE] Error: {e.msg}")
            logging.error(f"❌ [FAILURE] Context around error: {blocks_str[max(0, e.pos-50):e.pos+50]}")
            logging.error(f"❌ [FAILURE] Full blocks_str: {blocks_str}")
            raise

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
