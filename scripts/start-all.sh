#!/bin/bash

set -e

echo "🚀 Starting MLOps infrastructure..."

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null && ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed or not running."
    exit 1
fi

# Set Airflow UID if not set
if [ -z "$AIRFLOW_UID" ]; then
    export AIRFLOW_UID=$(id -u)
    echo "📝 Setting AIRFLOW_UID=$AIRFLOW_UID"
fi

# Start MinIO and Airflow services
echo "🐳 Starting Docker Compose services (MinIO, Airflow)..."
docker-compose up -d

echo "⏳ Waiting for services to be ready..."
sleep 10

# Check service status
echo ""
echo "📊 Service Status:"
docker-compose ps

echo ""
echo "✅ Services started!"
echo ""
echo "🌐 Access URLs:"
echo "   - MinIO Console:    http://localhost:9001 (minioadmin/minioadmin)"
echo "   - MinIO API:        http://localhost:9000"
echo "   - Airflow UI:       http://localhost:8080 (airflow/airflow)"
echo ""
echo "💡 To setup k3d cluster, run:"
echo "   ./scripts/setup-k3d.sh"
echo ""
echo "💡 To view logs, run:"
echo "   docker-compose logs -f"
echo ""
echo "💡 To stop services, run:"
echo "   ./scripts/stop-all.sh"
