import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from dotenv import load_dotenv
from google.cloud import bigquery

from utils.logging import cprint

# Load environment variables
load_dotenv()


def get_latency_check_query():
    with open("latency_check_query.sql", "r") as file:
        return file.read()


LATENCY_CHECK_QUERY = get_latency_check_query()
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
) -> list[dict[str, Any]]:
    """
    Process a single dataset for latency checks.

    Args:
        client: BigQuery client.
        project_name: Name of the BigQuery project.
        audit_dataset_name: Name of the audit dataset.
        latency_params_table: Name of the latency parameters table.
        dataset: Name of the dataset to process.
    """
    # First, get the list of tables in the dataset
    tables_query = f"""
    SELECT table_id
    FROM `{project_name}.{dataset}.__TABLES__`
    WHERE type = 'TABLE'
    """
    tables = [row["table_id"] for row in client.query(tables_query).result()]

    all_results = []
    for table in tables:
        dataset_query = LATENCY_CHECK_QUERY.format(
            project_name=project_name,
            audit_dataset_name=audit_dataset_name,
            latency_params_table=latency_params_table,
            dataset_id=dataset,
            table_id=table,
        )
        query_job = client.query(dataset_query)
        results = [dict(row) for row in query_job.result()]

        for row in results:
            update_info = row["update_info"]
            processed_row = {
                "project_id": row["project_id"],
                "dataset_id": row["dataset_id"],
                "table_id": row["table_id"],
                "threshold_hours": row["threshold_hours"],
                "inclusion_rule": row["inclusion_rule"],
                "group_by_column": row["group_by_column"],
                "last_updated_column": row["last_updated_column"],
                "last_modified_time": update_info["last_modified_time"],
                "hours_since_update": update_info["hours_since_update"],
                "group_by_value": update_info["group_by_value"],
            }
            all_results.append(processed_row)

    return all_results


def get_latency_data(
    client: bigquery.Client,
    project_name: str,
    audit_dataset_name: str,
    latency_params_table: str,
    target_dataset: str | None = None,
) -> list[dict[str, Any]]:
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
                cprint(f"Dataset {dataset} generated an exception: {exc}", severity="ERROR")

    cprint(f"Processed {len(all_results)} tables in total")
    return all_results
