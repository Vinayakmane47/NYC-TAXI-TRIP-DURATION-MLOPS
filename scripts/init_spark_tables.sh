#!/bin/bash
# Spark SQL Table Initialization Script
# This script creates tables in Spark SQL Thrift Server for BI access

set -e

SPARK_THRIFT_SERVER=${SPARK_THRIFT_SERVER:-spark-thrift-server:10000}
SQL_FILE=${SQL_FILE:-/opt/airflow/spark_jobs/create_tables.sql}

echo "Initializing Spark SQL tables..."
echo "Thrift Server: $SPARK_THRIFT_SERVER"
echo "SQL File: $SQL_FILE"

# Wait for Spark Thrift Server to be ready
echo "Waiting for Spark Thrift Server to be ready..."
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if nc -z spark-thrift-server 10000 2>/dev/null; then
        echo "Spark Thrift Server is ready!"
        break
    fi
    attempt=$((attempt + 1))
    echo "Attempt $attempt/$max_attempts: Spark Thrift Server not ready, waiting..."
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo "ERROR: Spark Thrift Server did not become ready in time"
    exit 1
fi

# Check if beeline (Hive CLI) is available
if ! command -v beeline &> /dev/null; then
    echo "beeline not found, using Spark SQL via Python..."
    
    # Use Python with PySpark to execute SQL
    python3 << EOF
from pyspark.sql import SparkSession
import sys

try:
    spark = SparkSession.builder \
        .appName("InitSparkTables") \
        .config("spark.sql.warehouse.dir", "s3a://nyc-taxi-gold/warehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    # Read SQL file
    with open("$SQL_FILE", "r") as f:
        sql_content = f.read()
    
    # Split by semicolons and execute each statement
    statements = [s.strip() for s in sql_content.split(";") if s.strip() and not s.strip().startswith("--")]
    
    for statement in statements:
        if statement:
            print(f"Executing: {statement[:50]}...")
            try:
                spark.sql(statement)
                print("✓ Success")
            except Exception as e:
                print(f"✗ Error: {e}")
                # Continue with other statements
    
    spark.stop()
    print("Table initialization completed!")
    
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)
EOF
else
    # Use beeline if available
    echo "Using beeline to execute SQL..."
    beeline -u "jdbc:hive2://$SPARK_THRIFT_SERVER/default" \
        -n "" \
        -p "" \
        -f "$SQL_FILE"
fi

echo "Spark SQL tables initialized successfully!"
