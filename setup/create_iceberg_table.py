import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True, help="GCS path to the input Parquet file.")
    parser.add_argument("--table_path", required=True, help="GCS path for the Iceberg table warehouse.")
    args = parser.parse_args()

    # **UPDATED:** Using a custom catalog name 'gcs_catalog'
    CATALOG_NAME = "gcs_catalog"
    CATALOG_WAREHOUSE = args.table_path.rsplit('/', 1)[0]

    spark = SparkSession.builder \
        .appName("Iceberg Table Initialization") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hadoop") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", CATALOG_WAREHOUSE) \
        .getOrCreate()

    print(f"Reading initial data from {args.input_path}")
    df = spark.read.parquet(args.input_path)

    # Simple transformation to add a month column for partitioning
    df = df.withColumn("month", df["tpep_pickup_datetime"].substr(1, 7))
    
    table_name = f"{CATALOG_NAME}.taxis"
    print(f"Writing initial Iceberg table to {table_name}")
    
    df.writeTo(table_name) \
        .partitionedBy("month") \
        .createOrReplace()

    print("Iceberg table created successfully.")
    spark.stop()

if __name__ == "__main__":
    main()