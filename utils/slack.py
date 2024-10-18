import json
import math
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from utils.log import cprint


def calculate_time_period(hours):
    if isinstance(hours, datetime):
        hours = (datetime.now(timezone.utc) - hours.astimezone(timezone.utc)).total_seconds() / 3600
    elif isinstance(hours, pd.Timestamp):
        hours = (pd.Timestamp.now(tz="UTC") - hours.tz_convert("UTC")).total_seconds() / 3600

    # Handle negative hours (future dates)
    hours = max(0, hours)

    days = hours / 24
    months = days / 30
    if months >= 1:
        rounded_months = math.floor(months)
        return f"{round(hours)} hours (≈ {rounded_months} month{'s' if rounded_months > 1 else ''})"
    elif days >= 1:
        rounded_days = math.floor(days)
        return f"{round(hours)} hours (≈ {rounded_days} day{'s' if rounded_days > 1 else ''})"
    else:
        return f"{round(hours)} hours"


def generate_slack_message(
    latency_data: list[dict[str, Any]],
    specific_dataset: Optional[str] = None,
    error_message: Optional[str] = None,
) -> dict:
    """
    Generate a Slack message summarizing the data latency.

    Args:
        latency_data: List of dictionaries containing latency data.
        specific_dataset: Optional; the specific dataset that was checked, if any.
        error_message: Optional; error message to include in the Slack message.

    Returns:
        dict: Slack message blocks summarizing the data latency.
    """
    cprint("Generating Slack message")

    message_blocks = []

    if error_message:
        message_blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"⚠️ *Error encountered during processing:*\n{error_message}"},
            }
        )
        message_blocks.append({"type": "divider"})

    if not latency_data:
        message_blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"All tables {'in the specified dataset ' if specific_dataset else ''}are up to date.",
                },
            }
        )
        return {"blocks": message_blocks}

    # Calculate summary statistics
    total_tables = len(latency_data)
    if total_tables == 0:
        cprint("No latency data found")
        message_blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "No tables found exceeding the latency threshold."}
        })
        return {"blocks": message_blocks}

    max_hours = max(row.get("hours_since_update", 0) for row in latency_data)
    avg_hours = sum(row.get("hours_since_update", 0) for row in latency_data) / total_tables

    # Group by dataset and calculate average delay
    dataset_stats = defaultdict(lambda: {"count": 0, "total_hours": 0})
    for row in latency_data:
        dataset = row["dataset_id"]
        dataset_stats[dataset]["count"] += 1
        dataset_stats[dataset]["total_hours"] += row["hours_since_update"]

    # Calculate average delay for each dataset
    for stats in dataset_stats.values():
        stats["avg_hours"] = stats["total_hours"] / stats["count"]

    # Sort datasets by average delay and get top 5
    top_datasets = sorted(dataset_stats.items(), key=lambda x: x[1]["avg_hours"], reverse=True)[:5]

    dataset_info = f"for dataset {specific_dataset} " if specific_dataset else ""

    message_blocks.extend(
        [
            {"type": "section", "text": {"type": "mrkdwn", "text": f"🚨 *Data Latency Alert {dataset_info.strip()}*"}},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*Tables breaching SLA:* {total_tables:,} tables\n"
                        f"*Max delay:* {calculate_time_period(round(max_hours))}\n"
                        f"*Average delay:* {calculate_time_period(round(avg_hours))}"
                    ),
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Top 5 datasets with highest average delay:*\n"
                    + "\n".join(
                        [
                            f"{i+1}. `{dataset}` - avg delay: {calculate_time_period(int(info['avg_hours']))} ({info['count']} tables)"
                            for i, (dataset, info) in enumerate(top_datasets)
                        ]
                    ),
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "📊 *Detailed Report*\nPlease check the thread below for a detailed Excel report of all outdated tables.",
                },
            },
        ]
    )

    cprint("Slack message blocks generated")
    cprint(f"Message blocks: {json.dumps(message_blocks)}", severity="DEBUG")
    return {"blocks": message_blocks}


def send_slack_message(message: dict, channel_id: str, token: str, file_paths: list[str] = None) -> None:
    try:
        client = WebClient(token=token)

        # Send the main message
        cprint("Sending main Slack message")

        # Extract blocks from the message dict
        blocks = message.get("blocks", [])

        # Create a fallback text from the first block if available
        fallback_text = "Data Latency Alert"
        if blocks and "text" in blocks[0].get("text", {}):
            fallback_text = blocks[0]["text"]["text"]

        response = client.chat_postMessage(channel=channel_id, text=fallback_text, blocks=blocks)

        # Get the timestamp of the main message to use as the thread_ts
        thread_ts = response["ts"]

        # Upload and share the Excel file as a reply in the thread
        if file_paths:
            for file_path in file_paths:
                cprint(f"Uploading file: {file_path}")
                with open(file_path, "rb") as file_content:
                    client.files_upload_v2(
                        channel=channel_id,
                        file=file_content,
                        filename=os.path.basename(file_path),
                        initial_comment="Here's the detailed report of outdated tables:",
                        thread_ts=thread_ts,
                    )

        cprint("Message and files sent successfully")
    except SlackApiError as e:
        error_message = e.response["error"] if isinstance(e.response, dict) else str(e.response)
        cprint(f"Error sending Slack message: {error_message}", severity="ERROR")
        raise


def write_to_excel(df: pd.DataFrame, excel_filename: str) -> str:
    """
    Writes data from DataFrame to Excel and adds a 'Read me' sheet.

    Args:
        df: DataFrame which needs to be written into Excel.
        excel_filename: Excel file to which df needs to be written.

    Returns:
        str: Name of the Excel file.
    """
    try:
        cprint("Starting Excel file writing process")

        # Convert timezone-aware datetimes to timezone-naive UTC
        for column in df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']).columns:
            df[column] = df[column].dt.tz_localize(None)

        with pd.ExcelWriter(excel_filename, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Results", index=False)

            read_me_data = {
                "Results Sheet": "This sheet provides details on tables that haven't been updated within the specified threshold.",
                "Recommendation": "Please review the 'Results' sheet to identify tables that may need attention. If any tables fall outside the defined threshold, consider investigating and taking appropriate actions."
            }
            read_me_df = pd.DataFrame(list(read_me_data.items()), columns=["Sheet", "Info"])
            read_me_df.to_excel(writer, sheet_name="Read me", index=False)

        cprint("Data saved to Excel file with additional sheets")
        return excel_filename
    except Exception as e:
        cprint(f"Error writing to Excel: {e}", severity="ERROR")
        raise
