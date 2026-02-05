#!/bin/bash
# Run feature engineering one month at a time to prevent OOM

YEAR=2025
MONTHS=(1 2 3 4)

for MONTH in "${MONTHS[@]}"; do
    echo "========================================="
    echo "Processing Month: $MONTH"
    echo "========================================="

    docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --driver-memory 1g \
      --executor-memory 1g \
      --conf spark.driver.maxResultSize=256m \
      --conf spark.sql.shuffle.partitions=4 \
      --conf spark.sql.adaptive.enabled=true \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=minioadmin \
      --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
      /opt/airflow/spark_jobs/feature_engineering_job.py \
      --year $YEAR --months $MONTH

    if [ $? -eq 0 ]; then
        echo "✓ Month $MONTH completed successfully"
    else
        echo "✗ Month $MONTH failed with error code $?"
        exit 1
    fi
done

echo "========================================="
echo "All months processed successfully!"
echo "========================================="
