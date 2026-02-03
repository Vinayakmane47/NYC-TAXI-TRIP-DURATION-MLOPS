#!/bin/bash

# Wait for MinIO to be ready
until mc alias set myminio http://localhost:9000 minioadmin minioadmin 2>/dev/null; do
  echo "Waiting for MinIO to be ready..."
  sleep 2
done

# Create bucket for DVC storage
echo "Creating DVC storage bucket..."
mc mb myminio/dvc-storage --ignore-existing || true

# Set bucket policy to public read (optional, adjust as needed)
mc anonymous set download myminio/dvc-storage || true

echo "MinIO initialization complete!"
echo "Bucket 'dvc-storage' created successfully."
