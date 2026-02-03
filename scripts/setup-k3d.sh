#!/bin/bash

set -e

CLUSTER_NAME="mlops-cluster"

echo "🚀 Setting up k3d cluster: $CLUSTER_NAME"

# Check if k3d is installed
if ! command -v k3d &> /dev/null; then
    echo "❌ k3d is not installed. Please install it first:"
    echo "   curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash"
    exit 1
fi

# Check if cluster already exists
if k3d cluster list | grep -q "$CLUSTER_NAME"; then
    echo "⚠️  Cluster $CLUSTER_NAME already exists."
    read -p "Do you want to delete and recreate it? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🗑️  Deleting existing cluster..."
        k3d cluster delete "$CLUSTER_NAME"
    else
        echo "✅ Using existing cluster."
        kubectl config use-context k3d-$CLUSTER_NAME
        exit 0
    fi
fi

# Create k3d cluster
echo "📦 Creating k3d cluster..."
k3d cluster create "$CLUSTER_NAME" \
  --port "8080:80@loadbalancer" \
  --port "8443:443@loadbalancer" \
  --port "5000:5000@loadbalancer" \
  --api-port 6443 \
  --servers 1 \
  --agents 1 \
  --wait \
  --timeout 300s

# Wait for cluster to be ready
echo "⏳ Waiting for cluster to be ready..."
kubectl wait --for=condition=ready node --all --timeout=120s || true

# Create namespace
echo "📝 Creating mlops namespace..."
kubectl create namespace mlops --dry-run=client -o yaml | kubectl apply -f -

# Set context
kubectl config use-context k3d-$CLUSTER_NAME

echo ""
echo "✅ k3d cluster '$CLUSTER_NAME' created successfully!"
echo ""
echo "📊 Cluster information:"
kubectl get nodes
echo ""
echo "📦 Namespaces:"
kubectl get namespaces
echo ""
echo "💡 To use this cluster, run:"
echo "   kubectl config use-context k3d-$CLUSTER_NAME"
echo ""
echo "💡 To delete this cluster, run:"
echo "   k3d cluster delete $CLUSTER_NAME"
