#!/usr/bin/env python3
"""
Export Gold layer to PostgreSQL WITHOUT Spark
Uses pandas + pyarrow to read Parquet directly from MinIO
"""
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import pyarrow.fs as fs
import sys

print("=" * 80)
print("Exporting Gold Layer to PostgreSQL (No-Spark Method)")
print("=" * 80)

# MinIO/S3 setup
s3 = fs.S3FileSystem(
    endpoint_override='minio:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    scheme='http'
)

# PostgreSQL connection
engine = create_engine('postgresql://airflow:airflow@airflow-postgres:5432/airflow')

print("\nReading Gold layer data from MinIO...")
try:
    # Read all parquet files from gold layer
    dataset = pq.ParquetDataset(
        'nyc-taxi-gold/gold/',
        filesystem=s3
    )

    # Read into pandas (with sampling if too large)
    table = dataset.read()
    df = table.to_pandas()

    print(f"Found {len(df):,} records")

    if len(df) == 0:
        print("No data found. Exiting.")
        sys.exit(0)

except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)

print("\nCreating aggregated views...")

# 1. Daily Summary
print("- gold_daily_trip_summary...")
daily_summary = df.groupby(['pickup_year', 'pickup_month', 'pickup_day_of_month']).agg({
    'trip_duration_min': 'mean',
    'trip_distance': 'mean',
    'fare_amount': 'mean',
    'total_amount': 'sum',
    'VendorID': 'count'  # trip count
}).rename(columns={'VendorID': 'trip_count'}).reset_index()

daily_summary.to_sql('gold_daily_trip_summary', engine, if_exists='replace', index=False)
print(f"  Created: {len(daily_summary)} rows")

# 2. Hourly Patterns
print("- gold_hourly_patterns...")
hourly_patterns = df.groupby(['pickup_hour', 'is_weekend', 'is_rush_hour']).agg({
    'trip_duration_min': 'mean',
    'fare_per_mile': 'mean',
    'VendorID': 'count'
}).rename(columns={'VendorID': 'trip_count'}).reset_index()

hourly_patterns.to_sql('gold_hourly_patterns', engine, if_exists='replace', index=False)
print(f"  Created: {len(hourly_patterns)} rows")

# 3. Pickup Location Stats
print("- gold_pickup_location_stats...")
pickup_stats = df.groupby('PULocationID').agg({
    'trip_distance': 'mean',
    'fare_amount': 'mean',
    'VendorID': 'count'
}).rename(columns={'VendorID': 'pickup_count'}).reset_index()

pickup_stats.to_sql('gold_pickup_location_stats', engine, if_exists='replace', index=False)
print(f"  Created: {len(pickup_stats)} rows")

# 4. Recent Trips (sample 1000)
print("- gold_recent_trips...")
recent_trips = df.nlargest(1000, 'tpep_pickup_datetime')[[
    'tpep_pickup_datetime', 'trip_duration_min', 'trip_distance',
    'fare_amount', 'PULocationID', 'DOLocationID',
    'is_weekend', 'is_rush_hour', 'passenger_count'
]]

recent_trips.to_sql('gold_recent_trips', engine, if_exists='replace', index=False)
print(f"  Created: {len(recent_trips)} rows")

print("\n" + "=" * 80)
print("SUCCESS! Tables created in PostgreSQL:")
print("  - gold_daily_trip_summary")
print("  - gold_hourly_patterns")
print("  - gold_pickup_location_stats")
print("  - gold_recent_trips")
print("\nNext: Connect Superset to PostgreSQL!")
print("=" * 80)
