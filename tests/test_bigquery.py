import os
from unittest.mock import Mock, call, patch
from datetime import datetime, timedelta, timezone

import pytest
from google.cloud import bigquery

from utils.bigquery import (
    LATENCY_CHECK_GROUP_BY,
    LATENCY_CHECK_TABLE_LEVEL,
    LATENCY_CHECK_DATASET_LEVEL,
    MAX_WORKERS,
    get_latency_data,
    process_dataset,
)

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
        {"dataset": "test_dataset1"},
        {"dataset": "test_dataset2"},
    ]
    return mock_job

def test_process_dataset(mock_bigquery_client):
    """
    Verifies that a single dataset is processed correctly.
    Crucial for ensuring accurate latency data for individual datasets.
    """
    current_time = datetime.now(timezone.utc)
    mock_bigquery_client.query.side_effect = [
        Mock(result=lambda: [{
            "dataset": "test_dataset1",
            "tables": None,
            "threshold_hours": 24,
            "group_by_column": None,
            "last_updated_column": None
        }]),
        Mock(result=lambda: [
            {
                "project_id": "test_project",
                "dataset_id": "test_dataset1",
                "table_id": "test_table1",
                "threshold_hours": 24,
                "last_updated_column": "last_modified_time",
                "last_modified_time": current_time - timedelta(hours=12),
                "hours_since_update": 12,
            },
            {
                "project_id": "test_project",
                "dataset_id": "test_dataset1",
                "table_id": "test_table2",
                "threshold_hours": 24,
                "last_updated_column": "last_modified_time",
                "last_modified_time": current_time - timedelta(hours=36),
                "hours_since_update": 36,
            },
        ]),
    ]
    result = process_dataset(mock_bigquery_client, "test_project", "audit_dataset", "latency_params", "test_dataset1")
    assert len(result) == 2
    assert result[0]["table_id"] == "test_table1"
    assert result[0]["hours_since_update"] == 12
    assert result[1]["table_id"] == "test_table2"
    assert result[1]["hours_since_update"] == 36

def test_process_dataset_with_group_by(mock_bigquery_client):
    """
    Tests processing a dataset with group_by configuration.
    """
    current_time = datetime.now(timezone.utc)
    mock_bigquery_client.query.side_effect = [
        Mock(result=lambda: [{
            "dataset": "test_dataset3",
            "tables": ["test_table3"],
            "threshold_hours": 6,
            "group_by_column": "brand",
            "last_updated_column": "last_updated_at"
        }]),
        Mock(result=lambda: [
            {
                "project_id": "test_project",
                "dataset_id": "test_dataset3",
                "table_id": "test_table3",
                "threshold_hours": 6,
                "group_by_column": "brand",
                "last_updated_column": "last_updated_at",
                "last_modified_time": current_time - timedelta(hours=3),
                "hours_since_update": 3,
                "group_by_value": "BrandA",
            },
            {
                "project_id": "test_project",
                "dataset_id": "test_dataset3",
                "table_id": "test_table3",
                "threshold_hours": 6,
                "group_by_column": "brand",
                "last_updated_column": "last_updated_at",
                "last_modified_time": current_time - timedelta(hours=12),
                "hours_since_update": 12,
                "group_by_value": "BrandB",
            },
        ]),
    ]

    result = process_dataset(mock_bigquery_client, "test_project", "audit_dataset", "latency_params", "test_dataset3")

    assert len(result) == 2
    assert result[0]["table_id"] == "test_table3"
    assert result[0]["group_by_value"] == "BrandA"
    assert result[0]["hours_since_update"] == 3
    assert result[1]["table_id"] == "test_table3"
    assert result[1]["group_by_value"] == "BrandB"
    assert result[1]["hours_since_update"] == 12

@patch("utils.bigquery.ThreadPoolExecutor")
@patch("utils.bigquery.as_completed")
def test_get_latency_data_success(
    mock_as_completed,
    mock_executor,
    mock_bigquery_client,
    mock_query_job,
):
    """
    Tests the parallel processing of multiple datasets.
    Essential for verifying the efficiency and correctness of the main latency data retrieval function.
    """
    mock_bigquery_client.query.return_value = mock_query_job
    mock_future = Mock()
    mock_future.result.return_value = [
        {"table_id": "table1", "hours_since_update": 1},
        {"table_id": "table2", "hours_since_update": 2}
    ]

    mock_executor.return_value.__enter__.return_value.submit.side_effect = [mock_future, mock_future]
    mock_as_completed.return_value = [mock_future, mock_future]

    result, errors = get_latency_data(mock_bigquery_client, "test_project", "audit_dataset", "latency_params")

    assert len(result) == 4  # 2 datasets * 2 tables per dataset
    assert len(errors) == 0
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
        mock_process.return_value = [{"table_id": "table1", "hours_since_update": 1}]
        result, errors = get_latency_data(
            mock_bigquery_client, "test_project", "audit_dataset", "latency_params", "target_dataset"
        )

    assert len(result) == 1
    assert len(errors) == 0
    assert result[0]["table_id"] == "table1"
    mock_bigquery_client.query.assert_called_once()

def test_get_latency_data_exception_handling(mock_bigquery_client, mock_query_job):
    """
    Tests the exception handling during dataset processing.
    Crucial for ensuring robustness when processing multiple datasets.
    """
    mock_bigquery_client.query.return_value = mock_query_job

    def mock_process_with_exception(client, project, audit, params, dataset):
        if dataset == "test_dataset2":
            raise Exception("Test exception")
        return [{"table_id": "table1", "hours_since_update": 1}]

    with patch("utils.bigquery.process_dataset", side_effect=mock_process_with_exception):
        with patch("utils.bigquery.cprint") as mock_cprint:
            result, errors = get_latency_data(mock_bigquery_client, "test_project", "audit_dataset", "latency_params")

    assert len(result) == 1  # Only one dataset processed successfully
    assert len(errors) == 1  # One error should be recorded
    mock_cprint.assert_any_call("Dataset test_dataset2 generated an exception: Test exception", severity="ERROR")

# Add more tests as needed for other scenarios
