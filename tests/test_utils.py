import base64
import json
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from freezegun import freeze_time
from google.cloud import bigquery

from utils.utils import generate_timestamped_filename, get_request_params, process_data_latency


def test_get_request_params_success():
    """Test successful decoding of request parameters from a Cloud Event."""
    test_params = {"key": "value"}
    encoded_params = base64.b64encode(json.dumps(test_params).encode()).decode()
    cloud_event = Mock(data={"message": {"data": encoded_params}})

    result = get_request_params(cloud_event)
    assert result == test_params


def test_get_request_params_invalid_format():
    """Test error handling for invalid message format in get_request_params."""
    cloud_event = Mock(data={"message": {"data": "invalid_data"}})

    with pytest.raises(ValueError) as exc_info:
        get_request_params(cloud_event)

    assert "Invalid message format" in str(exc_info.value)
    assert "Incorrect padding" in str(exc_info.value)


def test_get_request_params_missing_data():
    """Test error handling for missing data in the cloud event."""
    cloud_event = Mock(data={})

    with pytest.raises(ValueError) as exc_info:
        get_request_params(cloud_event)

    assert "Invalid message format" in str(exc_info.value)


@patch("utils.bigquery.get_latency_data")
def test_process_data_latency(mock_get_latency_data):
    """Test process_data_latency function for correct data processing."""
    mock_client = Mock(spec=bigquery.Client)
    mock_latency_data = [{"table": "table1", "latency": 5}, {"table": "table2", "latency": 10}]
    mock_get_latency_data.return_value = mock_latency_data

    latency_data, df = process_data_latency(
        mock_client,
        project_name="test_project",
        audit_dataset_name="test_audit_dataset",
        latency_params_table="test_params_table",
    )

    assert latency_data == mock_latency_data
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["table", "latency"]


@patch("utils.bigquery.get_latency_data")
def test_process_data_latency_specific_dataset(mock_get_latency_data):
    """Test process_data_latency function with a specific dataset."""
    mock_client = Mock(spec=bigquery.Client)
    mock_latency_data = [{"table": "table1", "latency": 5}]
    mock_get_latency_data.return_value = mock_latency_data

    latency_data, df = process_data_latency(
        mock_client,
        project_name="test_project",
        audit_dataset_name="test_audit_dataset",
        latency_params_table="test_params_table",
        target_dataset="test_dataset",
    )

    mock_get_latency_data.assert_called_once_with(
        mock_client,
        "test_project",
        "test_audit_dataset",
        "test_params_table",
        "test_dataset",
    )
    assert latency_data == mock_latency_data
    assert len(df) == 1


def test_process_data_latency_empty_result():
    """Test process_data_latency function when no latency data is returned."""
    mock_client = Mock(spec=bigquery.Client)
    with patch("utils.bigquery.get_latency_data", return_value=[]):
        latency_data, df = process_data_latency(
            mock_client,
            project_name="test_project",
            audit_dataset_name="test_audit_dataset",
            latency_params_table="test_params_table",
        )

    assert latency_data == []
    assert df.empty


@patch("utils.bigquery.get_latency_data")
def test_process_data_latency_various_data_types(mock_get_latency_data):
    """Test process_data_latency with various types of data returned from get_latency_data."""
    mock_client = Mock(spec=bigquery.Client)
    mock_latency_data = [
        {"table": "table1", "latency": 5, "extra_field": "value"},
        {"table": "table2", "latency": 10.5},
        {"table": "table3", "latency": "15"},
    ]
    mock_get_latency_data.return_value = mock_latency_data

    latency_data, df = process_data_latency(
        mock_client,
        project_name="test_project",
        audit_dataset_name="test_audit_dataset",
        latency_params_table="test_params_table",
    )

    assert latency_data == mock_latency_data
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df.columns) == {"table", "latency", "extra_field"}


def test_generate_timestamped_filename_basic():
    """Test basic filename generation with default timezone offset."""
    base_path = "/tmp/test.txt"
    with freeze_time("2024-01-01 12:00:00"):
        result = generate_timestamped_filename(base_path)
        assert result == "/tmp/test_20240101_173000.txt"


def test_generate_timestamped_filename_with_custom_tz():
    """Test filename generation with custom timezone offset."""
    base_path = "/tmp/test.xlsx"
    with freeze_time("2024-01-01 12:00:00"):
        result = generate_timestamped_filename(base_path, tz_offset=(0, 0))  # UTC
        assert result == "/tmp/test_20240101_120000.xlsx"


def test_generate_timestamped_filename_with_complex_path():
    """Test filename generation with complex file paths."""
    base_path = "/tmp/path/to/my.complex.file.txt"
    with freeze_time("2024-01-01 12:00:00"):
        result = generate_timestamped_filename(base_path)
        assert result == "/tmp/path/to/my.complex.file_20240101_173000.txt"


def test_generate_timestamped_filename_no_extension():
    """Test filename generation for files without extensions."""
    base_path = "/tmp/testfile"
    with freeze_time("2024-01-01 12:00:00"):
        result = generate_timestamped_filename(base_path)
        assert result == "/tmp/testfile_20240101_173000"


def test_generate_timestamped_filename_negative_offset():
    """Test filename generation with negative timezone offset."""
    base_path = "/tmp/test.txt"
    with freeze_time("2024-01-01 12:00:00"):
        result = generate_timestamped_filename(base_path, tz_offset=(-4, 0))  # UTC-4
        assert result == "/tmp/test_20240101_080000.txt"
