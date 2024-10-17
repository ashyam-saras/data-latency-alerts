import os
from unittest.mock import Mock, patch, mock_open
from datetime import datetime, timezone, timedelta

import pandas as pd
import pytest
from slack_sdk.errors import SlackApiError

from utils.slack import generate_slack_message, send_slack_message, write_to_excel, calculate_time_period


@pytest.fixture
def sample_latency_data():
    """Fixture to provide sample latency data for testing."""
    return [
        {"dataset_id": "dataset1", "table_id": "table1", "hours_since_update": 5},
        {"dataset_id": "dataset1", "table_id": "table2", "hours_since_update": 10},
    ]


def test_calculate_time_period():
    """Test calculate_time_period function with various inputs."""
    assert calculate_time_period(5) == "5 hours"
    assert calculate_time_period(25) == "25 hours (≈ 1 day)"
    assert calculate_time_period(50) == "50 hours (≈ 2 days)"
    assert calculate_time_period(750) == "750 hours (≈ 1 month)"
    assert calculate_time_period(1500) == "1500 hours (≈ 2 months)"

    # Test with datetime input
    now = datetime.now(timezone.utc)
    five_hours_ago = now - timedelta(hours=5)
    assert calculate_time_period(five_hours_ago) == "5 hours"

    # Test with pandas Timestamp input
    five_hours_ago_ts = pd.Timestamp.now(tz='UTC') - pd.Timedelta(hours=5)
    assert calculate_time_period(five_hours_ago_ts) == "5 hours"


def test_generate_slack_message(sample_latency_data):
    """
    Tests the generation of Slack messages for latency data.
    Ensures correct formatting and content of the message.
    """
    message = generate_slack_message(sample_latency_data)
    assert isinstance(message, dict)
    assert "blocks" in message
    blocks = message["blocks"]
    
    message_text = "\n".join(block["text"]["text"] for block in blocks if "text" in block)
    
    assert "*Data Latency Alert" in message_text
    assert "*Tables breaching SLA:* 2 tables" in message_text
    assert "*Max delay:* 10 hours" in message_text
    assert "*Average delay:* 8 hours" in message_text
    assert "Top 5 datasets with highest average delay:" in message_text
    assert "`dataset1` - avg delay: 7 hours (2 tables)" in message_text
    assert "Detailed Report" in message_text


def test_generate_slack_message_empty_data():
    """
    Tests message generation when no latency data is provided.
    Ensures appropriate message for up-to-date scenarios.
    """
    message = generate_slack_message([])
    assert isinstance(message, dict)
    assert "blocks" in message
    blocks = message["blocks"]
    message_text = "\n".join(block["text"]["text"] for block in blocks if "text" in block)
    assert "All tables are up to date." in message_text


def test_generate_slack_message_specific_dataset(sample_latency_data):
    """
    Tests message generation for a specific dataset.
    Verifies that the dataset name is included in the message.
    """
    message = generate_slack_message(sample_latency_data, specific_dataset="test_dataset")
    assert isinstance(message, dict)
    assert "blocks" in message
    blocks = message["blocks"]
    message_text = "\n".join(block["text"]["text"] for block in blocks if "text" in block)
    assert "Data Latency Alert for dataset test_dataset" in message_text


def test_generate_slack_message_with_error():
    """
    Tests message generation when an error message is provided.
    Ensures the error message is included in the Slack message.
    """
    error_message = "Test error occurred"
    message = generate_slack_message([], error_message=error_message)
    assert isinstance(message, dict)
    assert "blocks" in message
    blocks = message["blocks"]
    message_text = "\n".join(block["text"]["text"] for block in blocks if "text" in block)
    assert "Error encountered during processing" in message_text
    assert error_message in message_text


def test_generate_slack_message_group_by(sample_latency_data):
    """
    Tests message generation with group_by data.
    Ensures group_by values are correctly handled.
    """
    group_by_data = sample_latency_data + [
        {"dataset_id": "dataset1", "table_id": "table1", "hours_since_update": 15, "group_by_value": "group1"},
        {"dataset_id": "dataset1", "table_id": "table1", "hours_since_update": 20, "group_by_value": "group2"},
    ]
    message = generate_slack_message(group_by_data)
    assert isinstance(message, dict)
    assert "blocks" in message
    blocks = message["blocks"]
    message_text = "\n".join(block["text"]["text"] for block in blocks if "text" in block)
    assert "*Max delay:* 20 hours" in message_text
    assert "*Tables breaching SLA:* 4 tables" in message_text


@patch("utils.slack.WebClient")
def test_send_slack_message_success(mock_web_client):
    """
    Tests successful sending of a Slack message.
    Verifies that the WebClient is called correctly.
    """
    mock_client = Mock()
    mock_web_client.return_value = mock_client
    mock_client.chat_postMessage.return_value = {"ts": "1234567890.123456"}

    message = {"blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "Test message"}}]}
    send_slack_message(message, "test_channel", "fake_token")

    mock_client.chat_postMessage.assert_called_once()
    call_args = mock_client.chat_postMessage.call_args[1]
    assert call_args["channel"] == "test_channel"
    assert call_args["blocks"] == message["blocks"]


@patch("utils.slack.WebClient")
def test_send_slack_message_with_file(mock_web_client):
    """
    Tests sending a Slack message with an attached file.
    Ensures both message and file upload are handled correctly.
    """
    mock_client = Mock()
    mock_web_client.return_value = mock_client
    mock_client.chat_postMessage.return_value = {"ts": "1234567890.123456"}

    message = {"blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "Test message"}}]}
    with patch("builtins.open", mock_open(read_data="file content")):
        send_slack_message(message, "test_channel", "fake_token", ["test.xlsx"])

    mock_client.chat_postMessage.assert_called_once()
    mock_client.files_upload_v2.assert_called_once()


@patch("utils.slack.WebClient")
def test_send_slack_message_error(mock_web_client):
    """
    Tests error handling when sending a Slack message fails.
    Verifies that SlackApiError is raised and handled appropriately.
    """
    mock_client = Mock()
    mock_web_client.return_value = mock_client
    mock_client.chat_postMessage.side_effect = SlackApiError("Error", {"error": "Test error"})

    message = {"blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "Test message"}}]}
    with pytest.raises(SlackApiError):
        send_slack_message(message, "test_channel", "fake_token")


def test_write_to_excel(tmp_path):
    """
    Tests writing data to an Excel file.
    Ensures correct file creation and content, including the 'Read me' sheet.
    """
    df = pd.DataFrame({
        "col1": [1, 2],
        "col2": [3, 4],
        "date_col": [pd.Timestamp('2023-01-01', tz='UTC'), pd.Timestamp('2023-01-02', tz='UTC')]
    })
    excel_file = tmp_path / "test.xlsx"

    result = write_to_excel(df, str(excel_file))

    assert os.path.exists(result)

    # Read the Excel file and check its contents
    with pd.ExcelFile(result) as xls:
        assert "Results" in xls.sheet_names
        assert "Read me" in xls.sheet_names

        results_df = pd.read_excel(xls, "Results")
        assert results_df.shape == df.shape
        assert all(results_df.columns == df.columns)
        assert results_df['date_col'].dtype == 'datetime64[ns]'  # Ensure timezone info is removed

        read_me_df = pd.read_excel(xls, "Read me")
        assert "Results Sheet" in read_me_df["Sheet"].values
        assert "Recommendation" in read_me_df["Sheet"].values


def test_write_to_excel_error():
    """
    Tests error handling when writing to Excel fails.
    Verifies that exceptions during file writing are caught and raised.
    """
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

    with pytest.raises(Exception):
        write_to_excel(df, "/nonexistent/path/test.xlsx")


def test_write_to_excel_with_non_utc_timezone(tmp_path):
    """
    Tests writing data with non-UTC timezone to Excel.
    Ensures correct handling of different timezones.
    """
    df = pd.DataFrame({
        "date_col": [pd.Timestamp('2023-01-01', tz='US/Eastern'), pd.Timestamp('2023-01-02', tz='US/Pacific')]
    })
    excel_file = tmp_path / "test.xlsx"

    result = write_to_excel(df, str(excel_file))

    assert os.path.exists(result)
    with pd.ExcelFile(result) as xls:
        results_df = pd.read_excel(xls, "Results")
        assert results_df['date_col'].dtype == 'datetime64[ns]'  # Ensure timezone info is removed
        assert len(results_df) == 2
        # Check that the dates are correct (now in UTC)
        assert results_df['date_col'][0] == pd.Timestamp('2023-01-01 05:00:00')  # UTC equivalent of 2023-01-01 00:00:00 US/Eastern
        assert results_df['date_col'][1] == pd.Timestamp('2023-01-02 08:00:00')  # UTC equivalent of 2023-01-02 00:00:00 US/Pacific
