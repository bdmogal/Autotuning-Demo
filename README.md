# Serverless Spark Autotuning Demo: A Day in the Life

This package provides an end-to-end demo showcasing the value of Serverless Spark Autotuning. It simulates a daily feature engineering pipeline orchestrated by Airflow, running a series of distinct Spark jobs against a changing dataset.

The goal is to observe how autotuning intelligently adapts to different workload types (joins, scaling, skew, memory) and changing data volumes over time.

## Prerequisites

1.  **Google Cloud SDK:** Ensure you have `gcloud` installed and authenticated.
    ```bash
    gcloud auth login
    gcloud auth application-default login
    ```
2.  **Google Cloud Project:** A GCP project with the following APIs enabled:
    *   Dataproc API
    *   Cloud Storage API
    *   IAM API
    *   Cloud Composer API (or a standalone Airflow environment)
3.  **Permissions:** You need permissions to create GCS buckets, submit Spark jobs, and manage Airflow DAGs. `Dataproc Editor` and `Storage Admin` roles are recommended.
4.  **Python 3.8+:** With the `google-cloud-storage` library installed.
5.  **GCS Bucket:** A dedicated GCS bucket for this demo.
6.  **Airflow Environment:** A running Airflow environment (like Cloud Composer) capable of accessing your GCS bucket and submitting Spark jobs.

## Demo Structure

```
Autotuning-Demo/
├── README.md
├── setup/
│ ├── setup.sh
│ ├── create_iceberg_table.py
│ └── taxi_zone_lookup.csv
├── airflow/
│ └── spark_autotuning_demo_dag.py
├── spark_jobs/
│ ├── simulate_data_change.py
│ ├── job_a_joins.py
│ ├── job_b_scaling.py
│ ├── job_c_skew.py
│ └── job_d_memory.py
└── visualization/
└── generate_report.py
```

## Step-by-Step Instructions

### Step 1: Configure and Run the Setup Script

The setup script automates the initial data preparation.

1.  Navigate to the `setup` directory.
2.  Make the script executable: `chmod +x setup.sh`
3.  Run the script and follow the prompts:
    ```bash
    ./setup.sh
    ```
    This script will:
    *   Ask for your GCP Project ID, Region, and GCS Bucket name.
    *   Download three months of NYC Yellow Taxi trip data (Jan, Feb, Mar 2023) and the taxi zone lookup table.
    *   Upload all this data to your GCS bucket.
    *   Submit a Serverless Spark job (`create_iceberg_table.py`) to convert the raw Parquet files for January 2023 into an Apache Iceberg table, which will serve as the starting point for our pipeline.

### Step 2: Configure and Deploy the Airflow DAG

1.  **Upload Python & SQL files to GCS:** The Airflow DAG needs access to the Spark job scripts. Upload the entire `spark_jobs` directory to a location in your GCS bucket (e.g., `gs://<your-bucket>/code/spark_jobs/`).
2.  **Configure the DAG:** Open `airflow/spark_autotuning_demo_dag.py` and edit the `GCP_PROJECT_ID`, `GCP_REGION`, and `GCS_BUCKET` variables to match your environment.
3.  **Deploy the DAG:** Upload the configured `spark_autotuning_demo_dag.py` file to your Airflow environment's DAGs folder.

### Step 3: Run the Pipeline and Observe

1.  In the Airflow UI, un-pause the `spark_autotuning_demo` DAG.
2.  Trigger the DAG manually for the first run. This simulates "Day 1".
3.  Let the DAG run on its schedule (or trigger it manually) for 4-5 subsequent "days". Each run will first modify the dataset and then execute the four Spark jobs with autotuning enabled.
    *   **Run 1 (Day 1):** Establishes a baseline with January data.
    *   **Run 2 (Day 2):** Adds February data (simulates growth).
    *   **Run 3 (Day 3):** Adds March data, which is intentionally skewed (simulates changing data characteristics).
    *   **Run 4 (Day 4):** Removes January data (simulates a sliding window/reduction).

### Step 4: Visualize the Results

After several DAG runs are complete, you can generate a performance report.

1.  Make sure you have `pandas`, `matplotlib`, and `gcsfs` installed:
    ```bash
    pip install pandas matplotlib gcsfs
    ```
2.  Navigate to the `visualization` directory.
3.  Run the script, passing your GCS bucket name as an argument:
    ```bash
    python generate_report.py gs://<your-bucket>/
    ```
4.  The script will generate `.png` image files in the current directory (e.g., `performance_report.png`), plotting the duration of each job type over the different runs.

You will see a clear trend: the initial run sets a baseline, and subsequent runs become faster and more stable as Autotuning learns from historical executions and adapts to the specific needs of each job.
