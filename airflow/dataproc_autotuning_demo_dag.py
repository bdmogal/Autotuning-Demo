import datetime
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
PERFORMANCE_LOG_FILE = f"gs://{GCS_BUCKET}/logs/performance_log.csv"

# **UPDATED: Use spark.jars.packages property**
DATAPROC_RUNTIME_VERSION = "2.2"
ICEBERG_MAVEN_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
PYSPARK_PROPERTIES = {"spark.jars.packages": ICEBERG_MAVEN_PACKAGE}

# Autotuning Config
AUTOTUNE_CONFIG = { "autotuning_config": { "scenarios": ["SCALING", "MEMORY", "SHUFFLE"] } }
RUNTIME_CONFIG = {"version": DATAPROC_RUNTIME_VERSION}

# ... (rest of the DAG, including get_job_metrics, remains the same as the previous version) ...
# NOTE: The rest of the DAG code from the previous step is correct and does not need to be changed.
# The create_spark_job_operator and simulate_data_change tasks will correctly inherit
# the PYSPARK_PROPERTIES defined above.

# [The get_job_metrics function from the previous step goes here. It is unchanged.]
def get_job_metrics(**context):
    """Fetches metrics for completed Dataproc jobs and appends to a log file."""
    run_id = context["run_id"]
    ti = context["ti"]
    
    # Improved XCom parsing for batch_id
    def get_batch_id_from_xcom(task_id):
        xcom_value = ti.xcom_pull(task_ids=task_id, key="return_value")
        if xcom_value and "name" in xcom_value:
            return xcom_value["name"].split("/")[-1]
        return None

    batch_ids = {
        "job_a_joins": get_batch_id_from_xcom("run_spark_job_a_joins"),
        "job_b_scaling": get_batch_id_from_xcom("run_spark_job_b_scaling"),
        "job_c_skew": get_batch_id_from_xcom("run_spark_job_c_skew"),
        "job_d_memory": get_batch_id_from_xcom("run_spark_job_d_memory"),
    }

    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{GCP_REGION}-dataproc.googleapis.com:443"}
    )
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(PERFORMANCE_LOG_FILE.replace(f"gs://{GCS_BUCKET}/", ""))

    log_lines = "run_id,job_type,batch_id,duration_seconds,total_dcu_hours\n"
    if blob.exists():
        log_lines = ""

    for job_type, batch_id in batch_ids.items():
        if not batch_id:
            print(f"Could not find batch_id for {job_type}. Skipping.")
            continue
        request = dataproc_v1.GetBatchRequest(name=f"projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}/batches/{batch_id}")
        batch = client.get_batch(request=request)
        
        duration = 0
        if batch.runtime_info and batch.runtime_info.approximate_usage:
             if batch.runtime_info.approximate_usage.milli_dcu_seconds:
                duration = batch.runtime_info.approximate_usage.milli_dcu_seconds / 1000

        dcu_hours = batch.runtime_info.usage_metrics.total_dcu_hours if batch.runtime_info.usage_metrics else 0
        
        log_lines += f"{run_id},{job_type},{batch_id},{duration},{dcu_hours}\n"

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

    simulate_data_change = DataprocSubmitPySparkBatchOperator(
        task_id="simulate_data_change",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        main_python_file_uri=f"{SPARK_CODE_PATH}/simulate_data_change.py",
        properties=PYSPARK_PROPERTIES,
        runtime_config=RUNTIME_CONFIG,
        args=[
            f"--table_path={ICEBERG_TABLE_PATH}",
            f"--raw_data_path=gs://{GCS_BUCKET}/data/raw/",
            "--run_date={{ ds_nodash }}"
        ],
    )

    def create_spark_job_operator(task_id, job_script, job_type_arg):
        return DataprocSubmitPySparkBatchOperator(
            task_id=task_id,
            project_id=GCP_PROJECT_ID,
            region=GCP_REGION,
            main_python_file_uri=f"{SPARK_CODE_PATH}/{job_script}",
            properties=PYSPARK_PROPERTIES,
            runtime_config=RUNTIME_CONFIG,
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

    log_performance_metrics = PythonOperator(
        task_id="log_performance_metrics",
        python_callable=get_job_metrics,
    )
    
    spark_jobs = [run_spark_job_a, run_spark_job_b, run_spark_job_c, run_spark_job_d]
    simulate_data_change >> spark_jobs >> log_performance_metrics