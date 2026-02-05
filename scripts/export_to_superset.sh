#!/bin/bash
# Direct export from Gold layer to PostgreSQL for Superset
# This script runs spark-submit directly without Airflow complications

set -e

echo "================================================================================"
echo "Exporting Gold Layer to PostgreSQL for Superset"
echo "================================================================================"

docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1g \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --jars /opt/spark/jars/postgresql-42.2.18.jar \
  /opt/airflow/spark_jobs/export_to_postgres.py

echo ""
echo "================================================================================"
echo "Export Complete!"
echo "================================================================================"
echo ""
echo "Next steps:"
echo "1. Open Superset: http://localhost:8088 (admin/admin)"
echo "2. Go to Settings -> Database Connections"
echo "3. Add PostgreSQL connection:"
echo "   - Host: airflow-postgres"
echo "   - Port: 5432"
echo "   - Database: airflow"
echo "   - Username: airflow"
echo "   - Password: airflow"
echo "4. Query tables: gold_daily_trip_summary, gold_hourly_patterns, etc."
echo "================================================================================"
