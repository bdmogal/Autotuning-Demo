#!/bin/bash

# Simple color formatting
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# --- Robust Path Detection ---
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo -e "${GREEN}--- Dataproc Autotuning Demo Setup ---${NC}"

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

# --- Data Download ---
TEMP_DIR="temp_downloads"
mkdir -p $TEMP_DIR
cd $TEMP_DIR
echo -e "\n${GREEN}Downloading NYC Taxi data...${NC}"
wget -q --show-progress https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
wget -q --show-progress https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet
wget -q --show-progress https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet
wget -q --show-progress https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv -O taxi_zone_lookup.csv

# --- Data Upload to GCS ---
echo -e "\n${GREEN}Uploading data to GCS bucket: $GCS_BUCKET...${NC}"
gsutil mb -p $GCLOUD_PROJECT -l $GCLOUD_REGION $GCS_BUCKET >/dev/null 2>&1 || true
gsutil cp *.parquet "$GCS_BUCKET/data/raw/"
gsutil cp taxi_zone_lookup.csv "$GCS_BUCKET/data/raw/"

# --- Initial Iceberg Table Creation ---
echo -e "\n${GREEN}Submitting Dataproc job to create initial Iceberg table...${NC}"
BATCH_ID="iceberg-init-$(date +%s)"
CREATE_SCRIPT="$SCRIPT_DIR/create_iceberg_table.py"
gsutil cp "$CREATE_SCRIPT" "$GCS_BUCKET/code/"

# **CORRECTED:** Using --properties with spark.jars.packages
ICEBERG_MAVEN_PACKAGE="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"

gcloud dataproc batches submit pyspark \
    "$GCS_BUCKET/code/create_iceberg_table.py" \
    --batch="$BATCH_ID" \
    --project="$GCLOUD_PROJECT" \
    --region="$GCLOUD_REGION" \
    --subnet=default \
    --version=2.2 \
    --properties="spark.jars.packages=$ICEBERG_MAVEN_PACKAGE" \
    -- \
    --input_path="$GCS_BUCKET/data/raw/yellow_tripdata_2023-01.parquet" \
    --table_path="$GCS_BUCKET/warehouse/taxis"

# --- Cleanup ---
echo -e "\n${GREEN}Cleaning up local files...${NC}"
cd ..
rm -rf $TEMP_DIR

echo -e "\n${GREEN}--- Setup Complete! ---${NC}"
echo "You can now proceed to configure and deploy the Airflow DAG."