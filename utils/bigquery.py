import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv
from google.cloud import bigquery

from utils.logging import cprint

# Load environment variables
load_dotenv()


def get_query_content(filename):
    with open(filename, "r") as file:
        return file.read()


LATENCY_CHECK_DATASET_LEVEL = get_query_content("latency_check_dataset_level.sql")
LATENCY_CHECK_TABLE_LEVEL = get_query_content("latency_check_table_level.sql")
LATENCY_CHECK_GROUP_BY = get_query_content("latency_check_group_by.sql")

MAX_WORKERS = int(os.getenv("BQ_PARALLEL_DATASETS", "10"))

PROJECT_NAME = os.environ["PROJECT_NAME"]
AUDIT_DATASET_NAME = os.environ["AUDIT_DATASET_NAME"]
LATENCY_PARAMS_TABLE = os.environ["LATENCY_PARAMS_TABLE"]


def process_dataset(
    client: bigquery.Client,
    project_name: str,
    audit_dataset_name: str,
    latency_params_table: str,
    dataset: str,
) -> List[Dict[str, Any]]:
    # Get the configuration for this dataset
    config_query = f"""
    SELECT 
      dataset,
      tables,
      threshold_hours,
      group_by_column,
      last_updated_column
    FROM `{project_name}.{audit_dataset_name}.{latency_params_table}`
    WHERE dataset = '{dataset}'
    """
    config_job = client.query(config_query)
    config = list(config_job.result())[0]

    all_results = []

    if config["group_by_column"] and config["last_updated_column"]:
        # Use group by query
        for table in config["tables"]:
            query = LATENCY_CHECK_GROUP_BY.format(
                project_name=project_name,
                audit_dataset_name=audit_dataset_name,
                latency_params_table=latency_params_table,
                dataset_id=dataset,
                table_id=table,
                group_by_column=config["group_by_column"],
                last_updated_column=config["last_updated_column"],
            )
            query_job = client.query(query)
            results = [dict(row) for row in query_job.result()]
            all_results.extend(results)
    else:
        # Use table level or dataset level query
        query = LATENCY_CHECK_TABLE_LEVEL if config["tables"] else LATENCY_CHECK_DATASET_LEVEL
        query = query.format(
            project_name=project_name,
            audit_dataset_name=audit_dataset_name,
            latency_params_table=latency_params_table,
            dataset_id=dataset,
            tables=", ".join([f"'{table}'" for table in config["tables"]]) if config["tables"] else "NULL"
        )
        query_job = client.query(query)
        results = [dict(row) for row in query_job.result()]
        all_results.extend(results)

    # Ensure 'hours_since_update' is present and valid in all results
    for result in all_results:
        if "hours_since_update" not in result or result["hours_since_update"] is None:
            cprint(f"Warning: hours_since_update missing for {result.get('table_id', 'unknown table')}", severity="WARNING")
            result["hours_since_update"] = float('inf')  # or some default value
        else:
            result["hours_since_update"] = float(result["hours_since_update"])

    return all_results


def get_latency_data(
    client: bigquery.Client,
    project_name: str,
    audit_dataset_name: str,
    latency_params_table: str,
    target_dataset: str | None = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Get the latency data for all datasets or a specific dataset.

    Args:
        client: BigQuery client.
        project_name: Name of the BigQuery project.
        audit_dataset_name: Name of the audit dataset.
        latency_params_table: Name of the latency parameters table.
        target_dataset: Optional; if provided, only check this dataset.
    """
    cprint("Getting latency data")
    cprint(f"Project name: {project_name}", severity="DEBUG")
    cprint(f"Audit dataset name: {audit_dataset_name}", severity="DEBUG")
    cprint(f"Latency params table: {latency_params_table}", severity="DEBUG")
    cprint(f"Target dataset: {target_dataset}", severity="DEBUG")

    # Get the list of datasets from dataset_params
    dataset_query = f"""
    SELECT DISTINCT dataset
    FROM `{project_name}.{audit_dataset_name}.{latency_params_table}`
    """

    if target_dataset:
        dataset_query += f" WHERE dataset = '{target_dataset}'"

    dataset_job = client.query(dataset_query)
    datasets = [row["dataset"] for row in dataset_job.result()]

    if target_dataset and not datasets:
        raise ValueError(f"Specified dataset '{target_dataset}' not found or not configured for monitoring")

    cprint(f"Found {len(datasets)} dataset{'s' if len(datasets) != 1 else ''} to process")

    # Run the main query for each dataset in parallel and combine the results
    all_results = []
    errors = []
    cprint(f"Using {MAX_WORKERS} parallel workers")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_dataset = {
            executor.submit(
                process_dataset,
                client,
                project_name,
                audit_dataset_name,
                latency_params_table,
                dataset,
            ): dataset
            for dataset in datasets
        }
        for future in as_completed(future_to_dataset):
            dataset = future_to_dataset[future]
            try:
                results = future.result()
                all_results.extend(results)
                cprint(f"Processed dataset: {dataset} with {len(results)} tables", severity="DEBUG")
            except Exception as exc:
                error_msg = f"Dataset {dataset} generated an exception: {exc}"
                cprint(error_msg, severity="ERROR")
                errors.append(error_msg)

    cprint(f"Processed {len(all_results)} tables in total")
    return all_results, errors  # Return both results and errors
