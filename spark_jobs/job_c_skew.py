import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--lookup_path", required=True) # Unused
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Autotune Demo - Job C (Skew)").getOrCreate()
    
    df = spark.read.format("iceberg").load(args.table_path)

    # Group by a low-cardinality column, which can be prone to data skew
    # Payment type 1 (Credit card) is far more common than others
    result = df.groupBy("payment_type") \
        .agg(
            avg("tip_amount").alias("average_tip"),
            count("*").alias("trip_count")
        )

    result.write.mode("overwrite").parquet(args.output_path)
    spark.stop()

if __name__ == "__main__":
    main()