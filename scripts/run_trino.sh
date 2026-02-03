#!/bin/bash
# Standalone Trino Docker container setup script
# This script runs Trino in a Docker container with proper configuration

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Starting Trino in standalone Docker container..."

# Stop and remove existing Trino container if it exists
docker stop trino-coordinator 2>/dev/null || true
docker rm trino-coordinator 2>/dev/null || true

# Run Trino container
docker run -d \
  --name trino-coordinator \
  --network nyc-taxi-trip-duration-mlops_mlops-network \
  -p 8083:8080 \
  -v "${PROJECT_ROOT}/scripts/trino/config/config.properties:/etc/trino/config.properties:ro" \
  -v "${PROJECT_ROOT}/scripts/trino/catalog:/etc/trino/catalog:ro" \
  -e AWS_ACCESS_KEY_ID=minioadmin \
  -e AWS_SECRET_ACCESS_KEY=minioadmin \
  -e JAVA_TOOL_OPTIONS="-Dfs.s3a.endpoint=http://minio:9000 -Dfs.s3a.access.key=minioadmin -Dfs.s3a.secret.key=minioadmin -Dfs.s3a.path.style.access=true -Dfs.s3a.connection.ssl.enabled=false" \
  trinodb/trino:latest

echo "✅ Trino container started!"
echo "   Web UI: http://localhost:8083"
echo "   To view logs: docker logs -f trino-coordinator"
echo "   To stop: docker stop trino-coordinator"
