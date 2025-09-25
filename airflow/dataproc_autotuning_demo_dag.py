import datetime
import json
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitPySparkBatchOperator,
)
from airflow.operators.python import PythonOperator
from google.cloud import dataproc_v1, storage

# --- CONFIGURATION ---
GCP_PROJECT_ID = "your-gcp-project-id"
GCP_REGION = "your-gcp-region" # e.g., us-central1
GCS_BUCKET = "your-gcs-bucket-name"
# ---------------------

# GCS paths
SPARK_CODE_PATH = f"gs://{GCS_BUCKET}/code/spark_jobs"
ICEBERG_TABLE_PATH = f"gs://{GCS_BUCKET}/warehouse/taxis"
ICEBERG_WAREHOUSE = f"gs://{GCS_BUCKET}/warehouse"
PERFORMANCE_LOG_FILE = f"gs://{GCS_BUCKET}/logs/performance_log.csv"

# **UPDATED JAR PATH**
# The setup.sh script now uploads the JAR to this location in your bucket.
ICEBERG_JAR = f"gs://{GCS_BUCKET}/jars/iceberg-spark-runtime-3.1_2.12-1.2.1.jar"
PYSPARK_JARS_CONFIG = {"spark.jars": ICEBERG_JAR}

# Autotuning Config
AUTOTUNE_CONFIG = {
    "autotuning_config": {
        "scenarios": ["SCALING", "MEMORY", "SHUFFLE"]
    }
}

def get_job_metrics(**context):
    """Fetches metrics for completed Dataproc jobs and appends to a log file."""
    run_id = context["run_id"]
    ti = context["ti"]
    
    # Retrieve the batch IDs saved via XComs
    batch_ids = {
        "job_a_joins": json.loads(ti.xcom_pull(task_ids="run_spark_job_a_joins", key="return_value"))["name"].split("/")[-1],
        "job_b_scaling": json.loads(ti.xcom_pull(task_ids="run_spark_job_b_scaling", key="return_value"))["name"].split("/")[-1],
        "job_c_skew": json.loads(ti.xcom_pull(task_ids="run_spark_job_c_skew", key="return_value"))["name"].split("/")[-1],
        "job_d_memory": json.loads(ti.xcom_pull(task_ids="run_spark_job_d_memory", key="return_value"))["name"].split("/")[-1],
    }

    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{GCP_REGION}-dataproc.googleapis.com:443"}
    )
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(PERFORMANCE_LOG_FILE.replace(f"gs://{GCS_BUCKET}/", ""))

    log_lines = "run_id,job_type,batch_id,duration_seconds,total_dcu_hours\n"
    if blob.exists():
        log_lines = "" # Don't write header if file exists

    for job_type, batch_id in batch_ids.items():
        if not batch_id:
            continue
        request = dataproc_v1.GetBatchRequest(name=f"projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}/batches/{batch_id}")
        batch = client.get_batch(request=request)
        
        # Calculate duration more reliably
        if batch.state_time and batch.runtime_info.approximate_usage.milli_dcu_seconds:
            duration = batch.runtime_info.approximate_usage.milli_dcu_seconds / 1000
        else:
            duration = 0

        dcu_hours = batch.runtime_info.usage_metrics.total_dcu_hours if batch.runtime_info.usage_metrics else 0
        
        log_lines += f"{run_id},{job_type},{batch_id},{duration},{dcu_hours}\n"

    # Append to GCS file
    current_content = blob.download_as_text() if blob.exists() else ""
    blob.upload_from_string(current_content + log_lines)
    print(f"Successfully logged metrics to {PERFORMANCE_LOG_FILE}")


with DAG(
    dag_id="dataproc_autotuning_demo",
    start_date=datetime.datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dataproc", "autotuning", "demo"],
) as dag:

    # Task 1: Simulate the data changing for the current day's run
    # {{ ds_nodash }} is the Airflow macro for the execution date
    simulate_data_change = DataprocSubmitPySparkBatchOperator(
        task_id="simulate_data_change",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        main_python_file_uri=f"{SPARK_CODE_PATH}/simulate_data_change.py",
        properties=PYSPARK_JARS_CONFIG,
        args=[
            f"--table_path={ICEBERG_TABLE_PATH}",
            f"--raw_data_path=gs://{GCS_BUCKET}/data/raw/",
            "--run_date={{ ds_nodash }}"
        ],
    )

    # --- Feature Engineering Jobs ---

    def create_spark_job_operator(task_id, job_script, job_type_arg):
        return DataprocSubmitPySparkBatchOperator(
            task_id=task_id,
            project_id=GCP_PROJECT_ID,
            region=GCP_REGION,
            main_python_file_uri=f"{SPARK_CODE_PATH}/{job_script}",
            properties=PYSPARK_JARS_CONFIG,
            args=[
                f"--table_path={ICEBERG_TABLE_PATH}",
                f"--output_path=gs://{GCS_BUCKET}/data/output/{job_type_arg}/{{{{ ds_nodash }}}}",
                f"--lookup_path=gs://{GCS_BUCKET}/data/raw/taxi_zone_lookup.csv",
            ],
            batch_config=AUTOTUNE_CONFIG,
            do_xcom_push=True,
        )

    run_spark_job_a = create_spark_job_operator("run_spark_job_a_joins", "job_a_joins.py", "joins")
    run_spark_job_b = create_spark_job_operator("run_spark_job_b_scaling", "job_b_scaling.py", "scaling")
    run_spark_job_c = create_spark_job_operator("run_spark_job_c_skew", "job_c_skew.py", "skew")
    run_spark_job_d = create_spark_job_operator("run_spark_job_d_memory", "job_d_memory.py", "memory")

    # Task to log performance metrics
    log_performance_metrics = PythonOperator(
        task_id="log_performance_metrics",
        python_callable=get_job_metrics,
    )
    
    # Define DAG dependencies
    spark_jobs = [run_spark_job_a, run_spark_job_b, run_spark_job_c, run_spark_job_d]
    simulate_data_change >> spark_jobs >> log_performance_metrics