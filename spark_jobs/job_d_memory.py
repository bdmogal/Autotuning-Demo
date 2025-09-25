import argparse
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--lookup_path", required=True) # Unused
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Autotune Demo - Job D (Memory)").getOrCreate()
    
    df = spark.read.format("iceberg").load(args.table_path)

    # A memory-intensive window function to calculate a running total
    windowSpec = Window.partitionBy("PULocationID").orderBy("tpep_pickup_datetime")
    
    result = df.withColumn(
        "running_total_fare",
        sum("fare_amount").over(windowSpec)
    )

    result.select("PULocationID", "tpep_pickup_datetime", "fare_amount", "running_total_fare") \
        .write.mode("overwrite").parquet(args.output_path)
    spark.stop()

if __name__ == "__main__":
    main()