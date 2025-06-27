#!/bin/bash

# Deploy DAGs to Cloud Composer
# Usage: ./deploy_dags.sh [ENVIRONMENT_NAME] [LOCATION]

set -e

# Configuration
PROJECT_ID=${PROJECT_ID:-"insightsprod"}
ENVIRONMENT_NAME=${1:-"data-latency-composer"}
LOCATION=${2:-"us-central1"}
DAG_FOLDER="dags"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Validate inputs
if [ -z "$PROJECT_ID" ]; then
    print_error "PROJECT_ID is not set. Please set it as an environment variable or update the script."
    exit 1
fi

print_status "Deploying DAGs to Cloud Composer..."
print_status "Project ID: $PROJECT_ID"
print_status "Environment: $ENVIRONMENT_NAME"
print_status "Location: $LOCATION"

# Check if DAG folder exists
if [ ! -d "$DAG_FOLDER" ]; then
    print_error "DAG folder '$DAG_FOLDER' not found!"
    exit 1
fi

# Check if gcloud is installed and authenticated
if ! command -v gcloud &> /dev/null; then
    print_error "gcloud CLI is not installed. Please install it first."
    exit 1
fi

# Check authentication
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
    print_error "No active gcloud authentication found. Please run 'gcloud auth login'"
    exit 1
fi

# Set the project
print_status "Setting project to $PROJECT_ID..."
gcloud config set project $PROJECT_ID

# Check if Composer environment exists
print_status "Checking if Composer environment exists..."
if ! gcloud composer environments describe $ENVIRONMENT_NAME --location=$LOCATION &> /dev/null; then
    print_error "Composer environment '$ENVIRONMENT_NAME' not found in location '$LOCATION'"
    print_status "Available environments:"
    gcloud composer environments list --locations=$LOCATION
    exit 1
fi

# Upload DAGs
print_status "Uploading DAGs to Composer environment..."

# Get the DAG bucket
DAG_BUCKET=$(gcloud composer environments describe $ENVIRONMENT_NAME \
    --location=$LOCATION \
    --format="get(config.dagGcsPrefix)")

if [ -z "$DAG_BUCKET" ]; then
    print_error "Could not retrieve DAG bucket from Composer environment"
    exit 1
fi

print_status "DAG bucket: $DAG_BUCKET"

# Upload all Python files in the dags folder
for dag_file in $DAG_FOLDER/*.py; do
    if [ -f "$dag_file" ]; then
        print_status "Uploading $(basename $dag_file)..."
        gsutil cp "$dag_file" "$DAG_BUCKET/"
        
        if [ $? -eq 0 ]; then
            print_status "✅ Successfully uploaded $(basename $dag_file)"
        else
            print_error "❌ Failed to upload $(basename $dag_file)"
            exit 1
        fi
    fi
done

print_status "🎉 All DAGs uploaded successfully!"
print_status "You can view them in the Airflow UI at:"
print_status "https://console.cloud.google.com/composer/environments/detail/$LOCATION/$ENVIRONMENT_NAME"

# Optional: Set Airflow variables
print_status "Setting up Airflow Variables..."

# Note: You'll need to update these URLs and values based on your actual deployment
cat << EOF

To complete the setup, please set the following Airflow Variables in the Composer environment:

1. DATA_LATENCY_CLOUD_FUNCTION_URL: https://us-central1-$PROJECT_ID.cloudfunctions.net/data-latency-alerts
2. DATA_LATENCY_SLACK_CHANNEL_ID: Your Slack channel ID
3. DATA_LATENCY_SLACK_API_TOKEN: Your Slack API token
4. DATA_LATENCY_PROJECT_NAME: $PROJECT_ID

You can set these variables using the Airflow UI or gcloud commands:

gcloud composer environments run $ENVIRONMENT_NAME --location $LOCATION variables set -- DATA_LATENCY_CLOUD_FUNCTION_URL "https://us-central1-$PROJECT_ID.cloudfunctions.net/data-latency-alerts"
gcloud composer environments run $ENVIRONMENT_NAME --location $LOCATION variables set -- DATA_LATENCY_SLACK_CHANNEL_ID "YOUR_CHANNEL_ID"
gcloud composer environments run $ENVIRONMENT_NAME --location $LOCATION variables set -- DATA_LATENCY_SLACK_API_TOKEN "YOUR_SLACK_TOKEN"
gcloud composer environments run $ENVIRONMENT_NAME --location $LOCATION variables set -- DATA_LATENCY_PROJECT_NAME "$PROJECT_ID"

EOF

print_status "Deployment completed! 🚀" 