import base64
import json
from typing import Any

import pandas as pd
from google.cloud import bigquery

from utils import bigquery as bq
from utils.log import cprint


def get_request_params(cloud_event: dict[str, Any]) -> dict[str, Any]:
    """
    Extract and decode request parameters from a Cloud Event.

    Args:
        cloud_event: The Cloud Event containing the message data.

    Returns:
        Dict containing the decoded request parameters.

    Raises:
        ValueError: If the message format is invalid.
    """
    try:
        message_data = cloud_event.data["message"]["data"]
        decoded_data = base64.b64decode(message_data).decode("utf-8")
        return json.loads(decoded_data)
    except (KeyError, AttributeError, base64.binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as e:
        cprint(f"Error decoding message: {e}", severity="ERROR")
        raise ValueError(f"Invalid message format: {str(e)}") from e


def process_data_latency(
    client: bigquery.Client,
    project_name: str,
    audit_dataset_name: str,
    latency_params_table: str,
    target_dataset: str = None,
) -> tuple[list, pd.DataFrame]:
    """
    Process data latency for all datasets or a specific dataset.

    Args:
        client: BigQuery client.
        project_name: Name of the BigQuery project.
        audit_dataset_name: Name of the audit dataset.
        latency_params_table: Name of the latency parameters table.
        target_dataset: Optional; if provided, only check this dataset.

    Returns:
        Tuple containing the latency data list and a pandas DataFrame.
    """
    latency_data = bq.get_latency_data(client, project_name, audit_dataset_name, latency_params_table, target_dataset)
    df = pd.DataFrame(latency_data)
    return latency_data, df
