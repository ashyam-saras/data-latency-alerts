import os

import functions_framework
from dotenv import load_dotenv
from flask import Request, jsonify
from google.cloud import bigquery

import utils.slack as slack
from utils.logging import cprint
from utils.utils import process_data_latency

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


@functions_framework.http
def latency_alert(request: Request):
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            raise ValueError("No JSON data provided in the request")

        channel_id = request_json.get("channel_id", SLACK_CHANNEL_ID)
        token = request_json.get("slack_token", TOKEN)
        target_dataset = request_json.get("target_dataset")

        if not channel_id:
            raise ValueError("Missing required parameter: channel_id")
        if not token:
            raise ValueError("Missing required parameter: slack_token")

        latency_data, df = process_data_latency(
            bigquery_client,
            PROJECT_NAME,
            AUDIT_DATASET_NAME,
            LATENCY_PARAMS_TABLE,
            target_dataset,
        )
        excel_filename = slack.write_to_excel(df, "/tmp/latency_data.xlsx")
        message = slack.generate_slack_message(latency_data, target_dataset)
        slack.send_slack_message(message, channel_id, token, [excel_filename])

        cprint("Data latency alert processed successfully")
        return jsonify({"status": "success", "message": "Data latency alert processed successfully"}), 200
    except ValueError as ve:
        cprint(f"Validation error: {str(ve)}", severity="ERROR")
        return jsonify({"status": "error", "message": str(ve)}), 400
    except Exception as e:
        cprint(f"Error in latency_alert function: {e}", severity="ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500
