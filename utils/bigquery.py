import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv
from google.cloud import bigquery

from utils.log import cprint

# Load environment variables
load_dotenv()


def get_query_content(filename):
    sql_dir = Path(__file__).parent.parent / "sql"
    with open(sql_dir / filename, "r") as file:
        return file.read()


PATTERN_BASED_LATENCY_CHECK = get_query_content("pattern_based_latency_check.sql")

PROJECT_NAME = os.environ["PROJECT_NAME"]
AUDIT_DATASET_NAME = os.environ["AUDIT_DATASET_NAME"]


def get_latency_data(
    client: bigquery.Client,
    project_name: str,
    audit_dataset_name: str,
    target_dataset: str | None = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Get the latency data using the new pattern-based approach.

    Args:
        client: BigQuery client.
        project_name: Name of the BigQuery project.
        audit_dataset_name: Name of the audit dataset.
        target_dataset: Optional; if provided, filter results to this dataset only.

    Returns:
        Tuple containing the latency data list and any errors encountered.
    """
    cprint("Getting latency data using pattern-based approach")
    cprint(f"Project name: {project_name}", severity="DEBUG")
    cprint(f"Audit dataset name: {audit_dataset_name}", severity="DEBUG")
    cprint(f"Target dataset: {target_dataset}", severity="DEBUG")

    errors = []

    try:
        # Format the SQL query with project and dataset names
        query = PATTERN_BASED_LATENCY_CHECK.format(
            project_name=project_name,
            audit_dataset_name=audit_dataset_name,
        )

        cprint("Executing pattern-based latency check query", severity="DEBUG")
        query_job = client.query(query)
        results = [dict(row) for row in query_job.result()]

        # Filter by target dataset if specified
        if target_dataset:
            filtered_results = [result for result in results if result["dataset_id"] == target_dataset]
            cprint(f"Filtered results for dataset '{target_dataset}': {len(filtered_results)} tables")
            results = filtered_results

        cprint(f"Found {len(results)} tables exceeding latency thresholds")

        return results, errors

    except Exception as exc:
        error_msg = f"Error executing pattern-based latency check: {exc}"
        cprint(error_msg, severity="ERROR")
        errors.append(error_msg)
        return [], errors
