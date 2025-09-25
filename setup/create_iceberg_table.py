import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True, help="GCS path to the input Parquet file.")
    parser.add_argument("--table_path", required=True, help="GCS path for the Iceberg table.")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("Iceberg Table Initialization") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", args.table_path.rsplit('/', 1)[0]) \
        .getOrCreate()

    print(f"Reading initial data from {args.input_path}")
    df = spark.read.parquet(args.input_path)

    # Simple transformation to add a month column for partitioning
    df = df.withColumn("month", df["tpep_pickup_datetime"].substr(1, 7))

    print(f"Writing initial Iceberg table to {args.table_path}")
    df.writeTo("spark_catalog.taxis") \
        .partitionedBy("month") \
        .createOrReplace()

    print("Iceberg table created successfully.")
    spark.stop()

if __name__ == "__main__":
    main()