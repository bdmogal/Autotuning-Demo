import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table_path", required=True)
    parser.add_argument("--raw_data_path", required=True)
    parser.add_argument("--run_date", required=True, help="YYYYMMDD from Airflow")
    args = parser.parse_args()

    run_number = int(args.run_date[-2:]) # A simple way to get a day number for simulation

    spark = SparkSession.builder \
        .appName("Data Simulation") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", args.table_path.rsplit('/', 1)[0]) \
        .getOrCreate()
    
    table_name = "spark_catalog.taxis"

    # --- Simulation Logic ---
    # Day 1: Do nothing, use the initial table.
    if run_number == 1:
        print("Run 1: Using initial table state (Jan 2023). No changes made.")
    
    # Day 2: Simulate growth by adding Feb data.
    elif run_number == 2:
        print("Run 2: Simulating growth. Appending Feb 2023 data.")
        df_feb = spark.read.parquet(f"{args.raw_data_path}/yellow_tripdata_2023-02.parquet")
        df_feb = df_feb.withColumn("month", df_feb["tpep_pickup_datetime"].substr(1, 7))
        df_feb.writeTo(table_name).append()
        print("Append complete.")

    # Day 3: Simulate changing characteristics by adding skewed March data.
    elif run_number == 3:
        print("Run 3: Simulating skew. Appending Mar 2023 data.")
        # NOTE: March data is naturally a bit different, good enough for a demo.
        # A more advanced demo could artificially skew this data before writing.
        df_mar = spark.read.parquet(f"{args.raw_data_path}/yellow_tripdata_2023-03.parquet")
        df_mar = df_mar.withColumn("month", df_mar["tpep_pickup_datetime"].substr(1, 7))
        df_mar.writeTo(table_name).append()
        print("Append complete.")
    
    # Day 4: Simulate reduction by removing Jan data (sliding window).
    elif run_number == 4:
        print("Run 4: Simulating reduction. Deleting Jan 2023 data.")
        spark.sql(f"DELETE FROM {table_name} WHERE month = '2023-01'")
        print("Delete complete.")
    
    else:
        print(f"Run {run_number}: No data change action defined. Using current table state.")

    spark.stop()

if __name__ == "__main__":
    main()