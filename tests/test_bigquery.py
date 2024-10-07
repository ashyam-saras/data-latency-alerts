import os
from unittest.mock import Mock, mock_open, patch

import pytest
from google.cloud import bigquery

from utils.bigquery import MAX_WORKERS, get_latency_check_query, get_latency_data, process_dataset


# Fixtures
@pytest.fixture
def mock_bigquery_client():
    """Fixture to create a mock BigQuery client."""
    return Mock(spec=bigquery.Client)


@pytest.fixture
def mock_query_job():
    """Fixture to create a mock query job with predefined result."""
    mock_job = Mock()
    mock_job.result.return_value = [
        {"dataset": "dataset1"},
        {"dataset": "dataset2"},
    ]
    return mock_job


@pytest.fixture
def mock_process_dataset():
    """Fixture to create a mock process_dataset function with predefined return value."""
    return Mock(return_value=[{"table": "table1", "latency": 1}, {"table": "table2", "latency": 2}])


# Tests
def test_get_latency_check_query():
    """
    Ensures that the SQL query is correctly read from the file.
    Critical for the proper functioning of the latency check process.
    """
    mock_sql_content = "SELECT * FROM {dataset_id}"
    with patch("builtins.open", mock_open(read_data=mock_sql_content)):
        result = get_latency_check_query()
    assert result == mock_sql_content


def test_process_dataset(mock_bigquery_client):
    """
    Verifies that a single dataset is processed correctly.
    Crucial for ensuring accurate latency data for individual datasets.
    """
    mock_bigquery_client.query.return_value.result.return_value = [
        {"table": "table1", "latency": 1, "threshold_hours": 24},
        {"table": "table2", "latency": 2, "threshold_hours": 24},
    ]
    result = process_dataset(mock_bigquery_client, "project", "audit_dataset", "latency_params", "dataset1")
    assert len(result) == 2
    assert result[0]["table"] == "table1"
    assert result[1]["latency"] == 2


@patch("utils.bigquery.ThreadPoolExecutor")
@patch("utils.bigquery.as_completed")
def test_get_latency_data_success(
    mock_as_completed,
    mock_executor,
    mock_bigquery_client,
    mock_query_job,
    mock_process_dataset,
):
    """
    Tests the parallel processing of multiple datasets.
    Essential for verifying the efficiency and correctness of the main latency data retrieval function.
    """
    mock_bigquery_client.query.return_value = mock_query_job
    mock_future = Mock()
    mock_future.result.return_value = mock_process_dataset.return_value

    mock_executor.return_value.__enter__.return_value.submit.side_effect = [mock_future, mock_future]
    mock_as_completed.return_value = [mock_future, mock_future]

    result = get_latency_data(mock_bigquery_client, "project", "audit_dataset", "latency_params")

    assert len(result) == 4  # 2 datasets * 2 tables per dataset
    mock_bigquery_client.query.assert_called_once()
    assert mock_executor.call_args[1]["max_workers"] == MAX_WORKERS
    assert mock_as_completed.called


def test_get_latency_data_target_dataset(mock_bigquery_client, mock_query_job):
    """
    Checks the functionality when a specific target dataset is provided.
    Important for ensuring the system can focus on a single dataset when required.
    """
    mock_bigquery_client.query.return_value = mock_query_job
    mock_query_job.result.return_value = [{"dataset": "target_dataset"}]

    with patch("utils.bigquery.process_dataset") as mock_process:
        mock_process.return_value = [{"table": "table1", "latency": 1}]
        result = get_latency_data(mock_bigquery_client, "project", "audit_dataset", "latency_params", "target_dataset")

    assert len(result) == 1
    assert result[0]["table"] == "table1"
    mock_bigquery_client.query.assert_called_once()


def test_get_latency_data_no_datasets(mock_bigquery_client):
    """
    Tests error handling when no datasets are found.
    Critical for proper error reporting in edge cases.
    """
    mock_bigquery_client.query.return_value.result.return_value = []

    with pytest.raises(
        ValueError, match="Specified dataset 'non_existent' not found or not configured for monitoring"
    ):
        get_latency_data(mock_bigquery_client, "project", "audit_dataset", "latency_params", "non_existent")


@patch.dict(os.environ, {"BQ_PARALLEL_DATASETS": "5"})
def test_max_workers_from_env():
    """
    Verifies that the MAX_WORKERS value is correctly set from the environment variable.
    Important for ensuring configurable parallelism.
    """
    import importlib

    import utils.bigquery

    importlib.reload(utils.bigquery)

    assert utils.bigquery.MAX_WORKERS == 5

    os.environ.pop("BQ_PARALLEL_DATASETS", None)
    importlib.reload(utils.bigquery)


def test_get_latency_data_exception_handling(mock_bigquery_client, mock_query_job):
    """
    Tests the exception handling during dataset processing.
    Crucial for ensuring robustness when processing multiple datasets.
    """
    mock_bigquery_client.query.return_value = mock_query_job

    def mock_process_with_exception(client, project, audit, params, dataset):
        if dataset == "dataset2":
            raise Exception("Test exception")
        return [{"table": "table1", "latency": 1}]

    with patch("utils.bigquery.process_dataset", side_effect=mock_process_with_exception):
        with patch("utils.bigquery.cprint") as mock_cprint:
            result = get_latency_data(mock_bigquery_client, "project", "audit_dataset", "latency_params")

    assert len(result) == 1  # Only one dataset processed successfully
    mock_cprint.assert_any_call("Dataset dataset2 generated an exception: Test exception", severity="ERROR")


def test_default_max_workers():
    """
    Checks the default value of MAX_WORKERS.
    Ensures a sensible default for parallel processing when not explicitly configured.
    """
    assert MAX_WORKERS == 10
