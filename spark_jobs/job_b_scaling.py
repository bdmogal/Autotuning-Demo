import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--lookup_path", required=True) # Unused, for consistent signature
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Autotune Demo - Job B (Scaling)").getOrCreate()
    
    df = spark.read.format("iceberg").load(args.table_path)

    # A wide aggregation sensitive to total data volume
    result = df.groupBy("PULocationID", "payment_type") \
        .agg(
            sum("total_amount").alias("total_revenue"),
            count("*").alias("trip_count")
        )

    result.write.mode("overwrite").parquet(args.output_path)
    spark.stop()

if __name__ == "__main__":
    main()