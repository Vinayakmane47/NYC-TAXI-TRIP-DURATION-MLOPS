#!/bin/bash
# Initialization script for NYC Taxi MLOps services
# This runs automatically when you start docker compose

echo "🚀 Initializing NYC Taxi MLOps services..."

# Wait for services to be ready
sleep 5

# Create MinIO buckets if they don't exist
if command -v mc &> /dev/null; then
    echo "📦 Setting up MinIO buckets..."
    mc alias set myminio http://localhost:9000 minioadmin minioadmin
    mc mb myminio/nyc-taxi-bronze --ignore-existing
    mc mb myminio/nyc-taxi-silver --ignore-existing
    mc mb myminio/nyc-taxi-gold --ignore-existing
    echo "✅ MinIO buckets created"
fi

echo "✅ Services initialized successfully!"
echo ""
echo "📍 Access points:"
echo "   - Airflow UI: http://localhost:8080 (admin/admin)"
echo "   - MinIO UI: http://localhost:9000 (minioadmin/minioadmin)"
echo ""
echo "🎯 To trigger the ETL pipeline:"
echo "   1. Open Airflow UI"
echo "   2. Enable the 'nyc_taxi_etl_pipeline' DAG"
echo "   3. Click 'Trigger DAG'"
