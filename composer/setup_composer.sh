#!/bin/bash

# Setup Cloud Composer Environment for Data Latency Alerts
# Usage: ./setup_composer.sh [ENVIRONMENT_NAME] [LOCATION]

set -e

# Configuration
PROJECT_ID=${PROJECT_ID:-"insightsprod"}
ENVIRONMENT_NAME=${1:-"data-latency-composer"}
LOCATION=${2:-"us-central1"}
MACHINE_TYPE=${MACHINE_TYPE:-"n1-standard-1"}
NODE_COUNT=${NODE_COUNT:-"3"}
DISK_SIZE=${DISK_SIZE:-"20"}
PYTHON_VERSION=${PYTHON_VERSION:-"3"}

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

print_status "Setting up Cloud Composer Environment..."
print_status "Project ID: $PROJECT_ID"
print_status "Environment: $ENVIRONMENT_NAME"
print_status "Location: $LOCATION"
print_status "Machine Type: $MACHINE_TYPE"
print_status "Node Count: $NODE_COUNT"

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

# Enable required APIs
print_status "Enabling required APIs..."
gcloud services enable composer.googleapis.com
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudscheduler.googleapis.com

# Check if environment already exists
if gcloud composer environments describe $ENVIRONMENT_NAME --location=$LOCATION &> /dev/null; then
    print_warning "Composer environment '$ENVIRONMENT_NAME' already exists!"
    print_status "Skipping creation. If you want to recreate it, delete it first:"
    print_status "gcloud composer environments delete $ENVIRONMENT_NAME --location=$LOCATION"
    exit 0
fi

# Create the Composer environment
print_status "Creating Composer environment '$ENVIRONMENT_NAME'..."
print_warning "This may take 15-20 minutes..."

gcloud composer environments create $ENVIRONMENT_NAME \
    --location=$LOCATION \
    --machine-type=$MACHINE_TYPE \
    --node-count=$NODE_COUNT \
    --disk-size=$DISK_SIZE \
    --python-version=$PYTHON_VERSION \
    --enable-ip-alias \
    --enable-private-ip-environ \
    --master-ipv4-cidr=172.16.0.0/23 \
    --enable-master-authorized-networks

if [ $? -eq 0 ]; then
    print_status "✅ Composer environment created successfully!"
else
    print_error "❌ Failed to create Composer environment"
    exit 1
fi

# Wait for environment to be ready
print_status "Waiting for environment to be ready..."
while true; do
    STATE=$(gcloud composer environments describe $ENVIRONMENT_NAME \
        --location=$LOCATION \
        --format="get(state)")
    
    if [ "$STATE" = "RUNNING" ]; then
        print_status "✅ Environment is ready!"
        break
    elif [ "$STATE" = "ERROR" ]; then
        print_error "❌ Environment creation failed!"
        exit 1
    else
        print_status "Environment state: $STATE. Waiting..."
        sleep 30
    fi
done

# Install additional Python packages if requirements.txt exists
if [ -f "requirements.txt" ]; then
    print_status "Installing additional Python packages..."
    gcloud composer environments update $ENVIRONMENT_NAME \
        --location=$LOCATION \
        --update-pypi-packages-from-file=requirements.txt
fi

# Get environment details
print_status "Getting environment details..."
AIRFLOW_URI=$(gcloud composer environments describe $ENVIRONMENT_NAME \
    --location=$LOCATION \
    --format="get(config.airflowUri)")

DAG_BUCKET=$(gcloud composer environments describe $ENVIRONMENT_NAME \
    --location=$LOCATION \
    --format="get(config.dagGcsPrefix)")

print_status "🎉 Setup completed successfully!"
echo ""
print_status "Environment Details:"
print_status "Name: $ENVIRONMENT_NAME"
print_status "Location: $LOCATION"
print_status "Airflow UI: $AIRFLOW_URI"
print_status "DAG Bucket: $DAG_BUCKET"
echo ""
print_status "Next Steps:"
print_status "1. Deploy your DAGs using: ./deploy_dags.sh"
print_status "2. Set up Airflow Variables in the UI or using gcloud commands"
print_status "3. Enable your DAGs in the Airflow UI"
echo ""
print_status "Useful Commands:"
print_status "- View environment: gcloud composer environments describe $ENVIRONMENT_NAME --location=$LOCATION"
print_status "- Delete environment: gcloud composer environments delete $ENVIRONMENT_NAME --location=$LOCATION"
print_status "- Update environment: gcloud composer environments update $ENVIRONMENT_NAME --location=$LOCATION" 