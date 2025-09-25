#!/bin/bash

# Simple color formatting
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${GREEN}--- Spark Autotuning Demo Setup ---${NC}"

# Prompt for user configuration
read -p "Enter your GCP Project ID: " GCLOUD_PROJECT
read -p "Enter your GCP Region (e.g., us-central1): " GCLOUD_REGION
read -p "Enter your GCS Bucket name (e.g., gs://my-demo-bucket): " GCS_BUCKET

# Set gcloud config
gcloud config set project $GCLOUD_PROJECT
gcloud config set dataproc/region $GCLOUD_REGION

echo -e "\n${GREEN}Configuration set:${NC}"
echo "Project: $GCLOUD_PROJECT"
echo "Region: $GCLOUD_REGION"
echo "Bucket: $GCS_BUCKET"

# Create a temporary data directory
DATA_DIR="temp_data"
mkdir -p $DATA_DIR
cd $DATA_DIR

# --- Data Download ---
echo -e "\n${GREEN}Downloading NYC Taxi data (Jan, Feb, Mar 2023)...${NC}"
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet

echo -e "\n${GREEN}Downloading taxi zone lookup table...${NC}"
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv -O taxi_zone_lookup.csv

# --- Data Upload to GCS ---
echo -e "\n${GREEN}Uploading data to GCS bucket: $GCS_BUCKET...${NC}"
gsutil mb -p $GCLOUD_PROJECT -l $GCLOUD_REGION $GCS_BUCKET || true # Fails gracefully if bucket exists
gsutil cp *.parquet $GCS_BUCKET/data/raw/
gsutil cp taxi_zone_lookup.csv $GCS_BUCKET/data/raw/

# --- Initial Iceberg Table Creation ---
echo -e "\n${GREEN}Submitting Spark job to create initial Iceberg table...${NC}"
echo "This will create the table using only Jan 2023 data."

# Create a unique ID for the batch job
BATCH_ID="iceberg-init-$(date +%s)"

# Get the Python script from the parent directory
CREATE_SCRIPT="../create_iceberg_table.py"
gsutil cp $CREATE_SCRIPT $GCS_BUCKET/code/

gcloud dataproc batches submit pyspark \
    $GCS_BUCKET/code/create_iceberg_table.py \
    --batch=$BATCH_ID \
    --project=$GCLOUD_PROJECT \
    --region=$GCLOUD_REGION \
    --subnet=default \
    --jars=gs://spark-lib/iceberg/iceberg-spark-runtime-3.1_2.12-1.2.1.jar \
    -- \
    --input_path="$GCS_BUCKET/data/raw/yellow_tripdata_2023-01.parquet" \
    --table_path="$GCS_BUCKET/warehouse/taxis"

# --- Cleanup ---
echo -e "\n${GREEN}Cleaning up local files...${NC}"
cd ..
rm -rf $DATA_DIR

echo -e "\n${GREEN}--- Setup Complete! ---${NC}"
echo "You can now proceed to configure and deploy the Airflow DAG."
