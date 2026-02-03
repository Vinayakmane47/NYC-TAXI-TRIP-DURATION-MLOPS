#!/bin/bash

# Script to initialize MinIO bucket for DVC storage
# Run this after MinIO is up and running

BUCKET_NAME="dvc-storage"
MINIO_ENDPOINT="localhost:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"

echo "🔧 Initializing MinIO bucket: $BUCKET_NAME"

# Check if mc (MinIO client) is installed
if ! command -v mc &> /dev/null; then
    echo "📦 Installing MinIO client..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        brew install minio/stable/mc || echo "Please install mc manually: brew install minio/stable/mc"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        wget https://dl.min.io/client/mc/release/linux-amd64/mc -O /tmp/mc
        chmod +x /tmp/mc
        sudo mv /tmp/mc /usr/local/bin/mc || mv /tmp/mc ~/.local/bin/mc
    else
        echo "❌ Please install MinIO client manually from https://min.io/docs/minio/linux/reference/minio-mc.html"
        exit 1
    fi
fi

# Configure MinIO alias
echo "🔗 Configuring MinIO alias..."
mc alias set myminio http://$MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY

# Create bucket if it doesn't exist
echo "📦 Creating bucket: $BUCKET_NAME"
mc mb myminio/$BUCKET_NAME --ignore-existing || {
    echo "⚠️  Failed to create bucket. It might already exist or MinIO is not ready yet."
    echo "💡 You can create it manually via MinIO Console at http://localhost:9001"
    exit 1
}

echo "✅ Bucket '$BUCKET_NAME' created successfully!"
echo ""
echo "💡 You can now configure DVC to use MinIO:"
echo "   dvc remote add -d minio s3://$BUCKET_NAME --endpoint-url http://$MINIO_ENDPOINT"
echo "   dvc remote modify minio endpointurl http://$MINIO_ENDPOINT"
echo "   dvc remote modify minio access_key_id $MINIO_ACCESS_KEY"
echo "   dvc remote modify minio secret_access_key $MINIO_SECRET_KEY"
