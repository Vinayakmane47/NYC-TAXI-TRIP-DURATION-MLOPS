#!/bin/bash
# Complete pipeline for 2024 data ingestion and processing

set -e  # Exit on error

YEAR=2024
MONTHS="1,2,3,4,5,6,7,8,9,10,11,12"  # All 12 months, or specify: "1,2,3"

echo "========================================="
echo "NYC Taxi Pipeline for Year: $YEAR"
echo "Months: $MONTHS"
echo "========================================="

# Step 1: Data Ingestion (Download from NYC TLC)
echo ""
echo "Step 1/3: Data Ingestion (Downloading $YEAR data)..."
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1g \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  /opt/airflow/spark_jobs/data_ingestion_job.py \
  --year $YEAR --months $MONTHS

if [ $? -ne 0 ]; then
    echo "ERROR: Data ingestion failed!"
    exit 1
fi

echo "✓ Data ingestion completed successfully!"

# Step 2: Data Preprocessing (Bronze -> Silver)
echo ""
echo "Step 2/3: Data Preprocessing (Bronze -> Silver)..."
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1536m \
  --executor-memory 1536m \
  --conf spark.driver.maxResultSize=384m \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  /opt/airflow/spark_jobs/preprocessing_job.py \
  --year $YEAR

if [ $? -ne 0 ]; then
    echo "ERROR: Preprocessing failed!"
    exit 1
fi

echo "✓ Preprocessing completed successfully!"

# Step 3: Feature Engineering (Silver -> Gold)
echo ""
echo "Step 3/3: Feature Engineering (Silver -> Gold)..."
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1536m \
  --executor-memory 1536m \
  --conf spark.driver.maxResultSize=384m \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  /opt/airflow/spark_jobs/feature_engineering_job.py \
  --year $YEAR --months $MONTHS

if [ $? -ne 0 ]; then
    echo "ERROR: Feature engineering failed!"
    exit 1
fi

echo "✓ Feature engineering completed successfully!"

# Step 4: Export to PostgreSQL for Superset (Optional)
echo ""
echo "Step 4/4 (Optional): Export to PostgreSQL for Superset..."
read -p "Do you want to export to PostgreSQL for Superset? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    bash /opt/airflow/scripts/export_to_superset.sh
    echo "✓ Export to PostgreSQL completed!"
fi

echo ""
echo "========================================="
echo "✓ PIPELINE COMPLETED SUCCESSFULLY!"
echo "========================================="
echo ""
echo "Data is now available in MinIO:"
echo "  - Bronze: s3://nyc-taxi-bronze/$YEAR/"
echo "  - Silver: s3://nyc-taxi-silver/$YEAR/"
echo "  - Gold:   s3://nyc-taxi-gold/gold/$YEAR/"
echo ""
echo "To view the data:"
echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo "  - Superset: http://localhost:8088 (admin/admin)"
echo "========================================="
