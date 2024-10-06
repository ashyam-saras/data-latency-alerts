import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from utils import cprint


def generate_slack_message(latency_data: list[dict[str, Any]], specific_dataset: Optional[str] = None) -> str:
    """
    Generate a Slack message summarizing the data latency.

    Args:
        latency_data: List of dictionaries containing latency data.
        specific_dataset: Optional; the specific dataset that was checked, if any.

    Returns:
        str: Slack message summarizing the data latency.
    """
    cprint("Generating Slack message")

    if not latency_data:
        cprint("No latency data found")
        return f"All tables {'in the specified dataset ' if specific_dataset else ''}are up to date."

    total_tables = len(latency_data)
    max_hours = max(row["hours_since_update"] for row in latency_data)
    avg_hours = sum(row["hours_since_update"] for row in latency_data) / total_tables

    # Get current timestamp in IST
    now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=5, minutes=30)))
    timestamp = now.strftime(r"%Y-%m-%d %H:%M:%S IST")

    dataset_info = f"for dataset {specific_dataset} " if specific_dataset else ""
    message = (
        f"Data Latency Alert Summary {dataset_info}(as of {timestamp}):\n"
        f"• Total outdated tables: {total_tables}\n"
        f"• Most outdated table: {max_hours:.1f} hours\n"
        f"• Average delay: {avg_hours:.1f} hours\n\n"
        "Detailed report will be attached in the thread."
    )

    cprint(f"Slack message: {message}")
    return message


def send_slack_message(message: str, channel_id: str, token: str, file_paths: list[str] = None) -> None:
    try:
        client = WebClient(token=token)

        # Send the main message
        cprint("Sending main Slack message")
        response = client.chat_postMessage(channel=channel_id, text=message)

        # Get the timestamp of the main message to use as the thread_ts
        thread_ts = response["ts"]

        # Upload and share the Excel file as a reply in the thread
        if file_paths:
            for file_path in file_paths:
                cprint(f"Uploading file: {file_path}")
                with open(file_path, "rb") as file_content:
                    upload_response = client.files_upload_v2(
                        channels=channel_id,
                        file=file_content,
                        filename=os.path.basename(file_path),
                        initial_comment="Here's the detailed report of outdated tables:",
                        thread_ts=thread_ts,
                    )

        cprint("Message and files sent successfully")
    except SlackApiError as e:
        cprint(f"Error sending Slack message: {e.response['error']}", severity="ERROR")
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
        with pd.ExcelWriter(excel_filename, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Results", index=False)

            read_me_data = {
                "Results Sheet": "This sheet provides details on tables that haven't been updated within the specified threshold.",
                "Recommendation": "Please review the 'Results' sheet to identify tables that may need attention. If any tables fall outside the defined threshold, consider investigating and taking appropriate actions. If you think any of these tables need to be excluded, please update the same in Audit.latency_alerts_parms table",
            }
            read_me_df = pd.DataFrame(list(read_me_data.items()), columns=["Sheet", "Info"])
            read_me_df.to_excel(writer, sheet_name="Read me", index=False)

        cprint("Data saved to Excel file with additional sheets")
        return excel_filename
    except Exception as e:
        cprint(f"Error writing to Excel: {e}", severity="ERROR")
        raise
