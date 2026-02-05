#!/usr/bin/env python3
"""
Test reading Gold layer data
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test Gold Read") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("=" * 80)
print("Testing Gold Layer Read")
print("=" * 80)

# Test 1: Read from a specific month
print("\n1. Testing read from January 2025...")
try:
    df_jan = spark.read.parquet("s3a://nyc-taxi-gold/gold/2025/01/")
    count_jan = df_jan.count()
    print(f"   SUCCESS: {count_jan:,} records")
    print("\n   Schema:")
    df_jan.printSchema()
except Exception as e:
    print(f"   ERROR: {e}")

# Test 2: Try reading all months with wildcard
print("\n2. Testing read with wildcard pattern...")
try:
    df_all = spark.read.parquet("s3a://nyc-taxi-gold/gold/2025/*/")
    count_all = df_all.count()
    print(f"   SUCCESS: {count_all:,} records across all months")
except Exception as e:
    print(f"   ERROR: {e}")

# Test 3: Try with mergeSchema
print("\n3. Testing read with mergeSchema option...")
try:
    df_merged = spark.read.option("mergeSchema", "true").parquet("s3a://nyc-taxi-gold/gold/2025/*/")
    count_merged = df_merged.count()
    print(f"   SUCCESS: {count_merged:,} records with schema merging")
except Exception as e:
    print(f"   ERROR: {e}")

spark.stop()
