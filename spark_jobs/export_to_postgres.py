#!/usr/bin/env python3
"""
Export Gold layer data to PostgreSQL for Superset access
This creates materialized views in PostgreSQL that Superset can query
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, sum as spark_sum
import sys

# JDBC connection properties
jdbc_url = "jdbc:postgresql://airflow-postgres:5432/airflow"
connection_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Create Spark session with S3A configuration
spark = SparkSession.builder \
    .appName("Export to PostgreSQL") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("=" * 80)
print("Exporting Gold Layer to PostgreSQL")
print("=" * 80)

try:
    print("\nReading from Gold layer (s3a://nyc-taxi-gold/gold/2025/*/)...")
    # Read from Gold layer (Parquet in MinIO) with schema merging
    gold_df = spark.read.option("mergeSchema", "true").parquet("s3a://nyc-taxi-gold/gold/2025/*/")

    record_count = gold_df.count()
    print(f"Found {record_count:,} records in Gold layer")

    if record_count == 0:
        print("WARNING: No data found in Gold layer. Exiting.")
        sys.exit(0)

except Exception as e:
    print(f"ERROR: Failed to read Gold layer: {e}")
    print("Make sure your data pipeline has run and populated the Gold layer.")
    sys.exit(1)

# Create aggregated views for Superset

# 1. Daily trip summary
print("Creating daily_trip_summary...")
daily_summary = gold_df.groupBy(
    "pickup_year", "pickup_month", "pickup_day_of_month"
).agg(
    count("*").alias("trip_count"),
    avg("trip_duration_min").alias("avg_duration_min"),
    avg("trip_distance").alias("avg_distance"),
    avg("fare_amount").alias("avg_fare"),
    spark_sum("fare_amount").alias("total_revenue")
)

daily_summary.write.jdbc(
    url=jdbc_url,
    table="gold_daily_trip_summary",
    mode="overwrite",
    properties=connection_properties
)

# 2. Hourly patterns
print("Creating hourly_patterns...")
hourly_patterns = gold_df.groupBy(
    "pickup_hour", "is_weekend", "is_rush_hour"
).agg(
    count("*").alias("trip_count"),
    avg("trip_duration_min").alias("avg_duration_min"),
    avg("fare_per_mile").alias("avg_fare_per_mile")
)

hourly_patterns.write.jdbc(
    url=jdbc_url,
    table="gold_hourly_patterns",
    mode="overwrite",
    properties=connection_properties
)

# 3. Location popularity
print("Creating location_popularity...")
pickup_stats = gold_df.groupBy("PULocationID").agg(
    count("*").alias("pickup_count"),
    avg("trip_distance").alias("avg_distance_from_zone"),
    avg("fare_amount").alias("avg_fare_from_zone")
)

pickup_stats.write.jdbc(
    url=jdbc_url,
    table="gold_pickup_location_stats",
    mode="overwrite",
    properties=connection_properties
)

# 4. Latest 1000 trips for detailed view
print("Creating recent_trips...")
recent_trips = gold_df.orderBy(
    gold_df.tpep_pickup_datetime.desc()
).limit(1000).select(
    "tpep_pickup_datetime",
    "trip_duration_min",
    "trip_distance",
    "fare_amount",
    "PULocationID",
    "DOLocationID",
    "is_weekend",
    "is_rush_hour",
    "passenger_count"
)

recent_trips.write.jdbc(
    url=jdbc_url,
    table="gold_recent_trips",
    mode="overwrite",
    properties=connection_properties
)

print("SUCCESS: All tables exported successfully!")
print("\nTables created in PostgreSQL:")
print("  - gold_daily_trip_summary")
print("  - gold_hourly_patterns")
print("  - gold_pickup_location_stats")
print("  - gold_recent_trips")
print("\nYou can now query these tables from Superset!")

spark.stop()
