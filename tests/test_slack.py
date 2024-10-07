import os
from unittest.mock import Mock, patch, mock_open  # Add mock_open here

import pandas as pd
import pytest
from slack_sdk.errors import SlackApiError

from utils.slack import generate_slack_message, send_slack_message, write_to_excel


@pytest.fixture
def sample_latency_data():
    """Fixture to provide sample latency data for testing."""
    return [
        {"schema": "schema1", "table": "table1", "hours_since_update": 5},
        {"schema": "schema1", "table": "table2", "hours_since_update": 10},
    ]


def test_generate_slack_message(sample_latency_data):
    """
    Tests the generation of Slack messages for latency data.
    Ensures correct formatting and content of the message.
    """
    message = generate_slack_message(sample_latency_data)
    assert "Tables missing SLA: 2" in message
    assert "Most outdated table: 10 hours" in message
    assert "Average delay: 8 hours" in message
    assert "Top 5 oldest tables:" in message
    assert "schema1.table2: 10 hours" in message
    assert "schema1.table1: 5 hours" in message


def test_generate_slack_message_empty_data():
    """
    Tests message generation when no latency data is provided.
    Ensures appropriate message for up-to-date scenarios.
    """
    message = generate_slack_message([])
    assert "All tables are up to date." in message


def test_generate_slack_message_specific_dataset(sample_latency_data):
    """
    Tests message generation for a specific dataset.
    Verifies that the dataset name is included in the message.
    """
    message = generate_slack_message(sample_latency_data, specific_dataset="test_dataset")
    assert "for dataset test_dataset" in message


@patch("utils.slack.WebClient")
def test_send_slack_message_success(mock_web_client):
    """
    Tests successful sending of a Slack message.
    Verifies that the WebClient is called correctly.
    """
    mock_client = Mock()
    mock_web_client.return_value = mock_client
    mock_client.chat_postMessage.return_value = {"ts": "1234567890.123456"}

    send_slack_message("Test message", "test_channel", "fake_token")

    mock_client.chat_postMessage.assert_called_once_with(channel="test_channel", text="Test message")


@patch("utils.slack.WebClient")
def test_send_slack_message_with_file(mock_web_client):
    """
    Tests sending a Slack message with an attached file.
    Ensures both message and file upload are handled correctly.
    """
    mock_client = Mock()
    mock_web_client.return_value = mock_client
    mock_client.chat_postMessage.return_value = {"ts": "1234567890.123456"}

    with patch("builtins.open", mock_open(read_data="file content")):
        send_slack_message("Test message", "test_channel", "fake_token", ["test.xlsx"])

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

    with pytest.raises(SlackApiError):
        send_slack_message("Test message", "test_channel", "fake_token")


def test_write_to_excel(tmp_path):
    """
    Tests writing data to an Excel file.
    Ensures correct file creation and content, including the 'Read me' sheet.
    """
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    excel_file = tmp_path / "test.xlsx"

    result = write_to_excel(df, str(excel_file))

    assert os.path.exists(result)

    # Read the Excel file and check its contents
    with pd.ExcelFile(result) as xls:
        assert "Results" in xls.sheet_names
        assert "Read me" in xls.sheet_names

        results_df = pd.read_excel(xls, "Results")
        assert results_df.equals(df)

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
