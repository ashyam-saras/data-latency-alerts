from typing import Any, List, Optional

from google.cloud import bigquery

from utils import cprint


def get_latency_check_query():
    with open("latency_check_query.sql", "r") as file:
        return file.read()


LATENCY_CHECK_QUERY = get_latency_check_query()


def get_latency_data(
    client: bigquery.Client,
    project_name: str,
    audit_dataset_name: str,
    latency_params_table: str,
    target_dataset: Optional[str] = None,
) -> List[dict[str, Any]]:
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
    SELECT dp.dataset, dp.exclude_list, dp.threshhold_hours
    FROM `{project_name}.{audit_dataset_name}.{latency_params_table}` dp
    JOIN `{project_name}.INFORMATION_SCHEMA.SCHEMATA` s
        ON dp.dataset = s.schema_name
    """

    if target_dataset:
        dataset_query += f" WHERE dp.dataset = '{target_dataset}'"

    dataset_job = client.query(dataset_query)
    datasets = [row["dataset"] for row in dataset_job.result()]

    if target_dataset and not datasets:
        raise ValueError(f"Specified dataset '{target_dataset}' not found or not configured for monitoring")

    cprint(f"Found {len(datasets)} dataset{'s' if len(datasets) != 1 else ''} to process")

    # Run the main query for each dataset and combine the results
    all_results = []
    for dataset in datasets:
        cprint(f"Processing dataset: {dataset}", severity="DEBUG")
        dataset_query = LATENCY_CHECK_QUERY.format(
            project_name=project_name,
            audit_dataset_name=audit_dataset_name,
            latency_params_table=latency_params_table,
            dataset_id=dataset,
        )
        query_job = client.query(dataset_query)
        all_results.extend([dict(row) for row in query_job.result()])

    cprint(f"Processed {len(all_results)} tables")
    return all_results
