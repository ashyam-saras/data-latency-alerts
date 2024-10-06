import os

import functions_framework
from dotenv import load_dotenv
from google.cloud import bigquery

import utils.slack as slack
from utils.logging import cprint
from utils.utils import get_request_params, process_data_latency

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

        latency_data, df = process_data_latency(
            bigquery_client,
            PROJECT_NAME,
            AUDIT_DATASET_NAME,
            LATENCY_PARAMS_TABLE,
            target_dataset,
        )
        excel_filename = slack.write_to_excel(df, "latency_data.xlsx")
        message = slack.generate_slack_message(latency_data, target_dataset)
        slack.send_slack_message(message, channel_id, token, [excel_filename])

        cprint("Data latency alert processed successfully")
    except Exception as e:
        cprint(f"Error in latency_alert function: {e}", severity="ERROR")
        raise
