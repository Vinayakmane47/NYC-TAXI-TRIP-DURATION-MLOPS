#!/bin/bash

set -e

echo "🛑 Stopping MLOps infrastructure..."

# Stop Docker Compose services
echo "🐳 Stopping Docker Compose services..."
docker-compose down

# Check if k3d cluster exists and ask to delete
if command -v k3d &> /dev/null; then
    if k3d cluster list | grep -q "mlops-cluster"; then
        echo ""
        read -p "Do you want to delete the k3d cluster? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "🗑️  Deleting k3d cluster..."
            k3d cluster delete mlops-cluster
        else
            echo "ℹ️  k3d cluster kept running."
        fi
    fi
fi

echo ""
echo "✅ All services stopped!"
