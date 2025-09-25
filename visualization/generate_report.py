import sys
import pandas as pd
import matplotlib.pyplot as plt
import gcsfs

def main():
    if len(sys.argv) < 2:
        print("Usage: python generate_report.py gs://<your-bucket-name>/")
        sys.exit(1)

    gcs_bucket_path = sys.argv[1]
    log_file_path = f"{gcs_bucket_path}logs/performance_log.csv"

    print(f"Reading performance log from: {log_file_path}")
    
    try:
        df = pd.read_csv(log_file_path)
    except FileNotFoundError:
        print(f"Error: Log file not found at {log_file_path}")
        print("Please ensure the Airflow DAG has run at least once.")
        sys.exit(1)

    # Extract a simple run number for plotting
    df['run_number'] = df['run_id'].str.extract(r'(\d+)$').astype(int)
    df = df.sort_values('run_number')

    # Create the plot
    fig, axes = plt.subplots(2, 2, figsize=(16, 12), sharex=True)
    fig.suptitle('Dataproc Autotuning Performance Over Time', fontsize=16)
    
    job_types = {
        "job_a_joins": axes[0, 0],
        "job_b_scaling": axes[0, 1],
        "job_c_skew": axes[1, 0],
        "job_d_memory": axes[1, 1],
    }

    for job, ax in job_types.items():
        subset = df[df['job_type'] == job]
        ax.plot(subset['run_number'], subset['duration_seconds'], marker='o', linestyle='-')
        ax.set_title(f'Performance of: {job}')
        ax.set_ylabel('Job Duration (seconds)')
        ax.set_xlabel('DAG Run Number (Day)')
        ax.grid(True)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    
    # Save the report
    output_filename = "performance_report.png"
    plt.savefig(output_filename)
    print(f"Report saved as {output_filename}")

if __name__ == "__main__":
    main()