import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--lookup_path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Autotune Demo - Job A (Joins)").getOrCreate()
    
    df_trips = spark.read.format("iceberg").load(args.table_path)
    df_zones = spark.read.option("header", "true").csv(args.lookup_path)

    # A join-heavy operation to enrich the trip data
    result = df_trips.join(df_zones, df_trips["PULocationID"] == df_zones["LocationID"], "inner") \
        .withColumnRenamed("Borough", "PickupBorough") \
        .drop("LocationID", "Zone", "service_zone")

    result.write.mode("overwrite").parquet(args.output_path)
    spark.stop()

if __name__ == "__main__":
    main()