#!/usr/bin/env python3
"""
Initialize Spark SQL tables for Bronze/Silver/Gold layers
This script creates external tables pointing to Parquet files in MinIO
"""
from pyspark.sql import SparkSession

# Create Spark session with S3A configuration
spark = SparkSession.builder \
    .appName("Initialize Lakehouse Tables") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.warehouse.dir", "s3a://nyc-taxi-gold/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

print("=" * 80)
print("Creating NYC Taxi Lakehouse Tables")
print("=" * 80)

# Read and execute the SQL file
with open('/opt/airflow/spark_jobs/create_tables.sql', 'r') as f:
    sql_content = f.read()

# Split by semicolon and execute each statement
statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip() and not stmt.strip().startswith('--')]

for i, statement in enumerate(statements, 1):
    # Skip empty statements and comments
    if not statement or statement.startswith('--'):
        continue

    print(f"\n[{i}/{len(statements)}] Executing SQL statement...")
    print(f"Statement: {statement[:100]}..." if len(statement) > 100 else f"Statement: {statement}")

    try:
        spark.sql(statement)
        print("✓ Success")
    except Exception as e:
        print(f"✗ Error: {e}")
        # Continue with other statements even if one fails
        continue

print("\n" + "=" * 80)
print("Verifying tables...")
print("=" * 80)

# Show databases
print("\nDatabases:")
spark.sql("SHOW DATABASES").show()

# Use nyc_taxi database
spark.sql("USE nyc_taxi")

# Show tables
print("\nTables in nyc_taxi database:")
spark.sql("SHOW TABLES").show()

# Try to describe each table
for table in ['bronze_trips', 'silver_trips', 'gold_trips']:
    print(f"\n{table.upper()} table structure:")
    try:
        spark.sql(f"DESCRIBE {table}").show(10, truncate=False)

        # Try to count rows if data exists
        try:
            count = spark.sql(f"SELECT COUNT(*) as count FROM {table}").collect()[0]['count']
            print(f"Row count: {count:,}")
        except Exception as e:
            print(f"No data found (this is OK if you haven't loaded data yet): {e}")
    except Exception as e:
        print(f"Error describing table: {e}")

print("\n" + "=" * 80)
print("Table initialization complete!")
print("=" * 80)
print("\nNext steps:")
print("1. Load data into the tables using your Airflow DAGs")
print("2. Run: MSCK REPAIR TABLE <table_name> to discover partitions")
print("3. Connect Superset to query these tables")
print("=" * 80)

spark.stop()
