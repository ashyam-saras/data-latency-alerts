import base64
import json
import os

import functions_framework
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

import utils.bigquery as bq
import utils.slack as slack
from utils import cprint

# Load environment variables
load_dotenv()

# Environment variables
PROJECT_NAME = os.environ["PROJECT_NAME"]
AUDIT_DATASET_NAME = os.environ["AUDIT_DATASET_NAME"]
LATENCY_PARAMS_TABLE = os.environ["LATENCY_PARAMS_TABLE"]
SLACK_CHANNEL_ID = os.environ["SLACK_CHANNEL_ID"]
TOKEN = os.environ["SLACK_API_TOKEN"]

# Initialize BigQuery client
bigquery_client = bigquery.Client()


def get_request_params(cloud_event):
    try:
        return json.loads(base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8"))
    except (KeyError, json.JSONDecodeError, UnicodeDecodeError) as e:
        cprint(f"Error decoding message: {e}", severity="ERROR")
        raise ValueError("Invalid message format") from e


def process_data_latency(client, specific_dataset=None):
    latency_data = bq.get_latency_data(client, PROJECT_NAME, AUDIT_DATASET_NAME, LATENCY_PARAMS_TABLE, specific_dataset)
    df = pd.DataFrame(latency_data)
    return latency_data, df


@functions_framework.cloud_event
def latency_alert(cloud_event):
    try:
        request_params = get_request_params(cloud_event)
        cprint(f"Request Params: {request_params}")

        channel_id = request_params.get("channel_id") or SLACK_CHANNEL_ID
        token = request_params.get("slack_token") or TOKEN
        target_dataset = request_params.get("target_dataset")

        if not channel_id:
            raise ValueError("Missing required parameter: channel_id")

        latency_data, df = process_data_latency(bigquery_client, target_dataset)
        excel_filename = slack.write_to_excel(df, "latency_data.xlsx")
        message = slack.generate_slack_message(latency_data, target_dataset)
        slack.send_slack_message(message, channel_id, token, [excel_filename])

        cprint("Data latency alert processed successfully")
    except Exception as e:
        cprint(f"Error in latency_alert function: {e}", severity="ERROR")
        raise
