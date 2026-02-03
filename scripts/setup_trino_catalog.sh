#!/bin/bash
# Setup Trino Iceberg catalog configuration
# This script ensures the Trino catalog is properly configured for Iceberg tables

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CATALOG_DIR="$PROJECT_ROOT/scripts/trino/catalog"

echo "Setting up Trino Iceberg catalog configuration..."

# Create catalog directory if it doesn't exist
mkdir -p "$CATALOG_DIR"

# Create iceberg.properties file
cat > "$CATALOG_DIR/iceberg.properties" << 'EOF'
connector.name=iceberg
iceberg.catalog.type=HADOOP
iceberg.catalog.warehouse=s3a://nyc-taxi-gold/iceberg/
iceberg.s3.endpoint=http://minio:9000
iceberg.s3.aws-access-key=minioadmin
iceberg.s3.aws-secret-key=minioadmin
iceberg.s3.path-style-access=true
iceberg.s3.ssl.enabled=false
EOF

echo "✅ Trino Iceberg catalog configuration created at: $CATALOG_DIR/iceberg.properties"
echo ""
echo "Catalog configuration:"
echo "  - Connector: iceberg"
echo "  - Catalog type: hadoop"
echo "  - Warehouse: s3a://nyc-taxi-gold/iceberg/"
echo "  - S3 endpoint: http://minio:9000"
echo ""
echo "To use this configuration:"
echo "  1. Mount the catalog directory in docker-compose.yml (already configured)"
echo "  2. Restart Trino coordinator service"
echo "  3. Tables will be accessible as: iceberg.<schema>.<table>"
