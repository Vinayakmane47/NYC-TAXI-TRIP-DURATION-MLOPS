# MLOps Local Setup Guide

This guide explains how to set up and run the MLOps infrastructure locally using Docker Compose (MinIO, Airflow) and k3d (local Kubernetes).

## Prerequisites

1. **Docker & Docker Compose**
   - Install Docker Desktop: https://www.docker.com/products/docker-desktop
   - Verify: `docker --version` and `docker-compose --version`

2. **k3d** (for local Kubernetes)
   ```bash
   curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
   ```

3. **kubectl** (Kubernetes CLI)
   ```bash
   # macOS
   brew install kubectl
   
   # Linux
   curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
   sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
   ```

4. **MinIO Client (mc)** - Optional, for bucket initialization
   ```bash
   # macOS
   brew install minio/stable/mc
   
   # Linux
   wget https://dl.min.io/client/mc/release/linux-amd64/mc
   chmod +x mc
   sudo mv mc /usr/local/bin/
   ```

## Quick Start

### 1. Start All Services

Simply run:

```bash
docker-compose up -d
```

This will automatically start:
- **MinIO** on ports 9000 (API) and 9001 (Console)
  - Automatically creates `dvc-storage` bucket on startup
- **Airflow** on port 8080 (Web UI)
  - Automatically initializes database and creates admin user
- **PostgreSQL** (Airflow metadata database)
- **Redis** (Airflow Celery broker)

All services will start in the correct order with proper health checks and dependencies.

### 2. Verify Services

Check that all services are running:

```bash
docker-compose ps
```

You should see all services in "Up" or "healthy" state.

### 3. Configure DVC with MinIO

```bash
# Remove existing local remote (if needed)
dvc remote remove mylocal  # optional

# Add MinIO as remote
dvc remote add -d minio s3://dvc-storage --endpoint-url http://localhost:9000

# Configure credentials
dvc remote modify minio endpointurl http://localhost:9000
dvc remote modify minio access_key_id minioadmin
dvc remote modify minio secret_access_key minioadmin

# Verify configuration
dvc remote list
```

### 4. Setup k3d Cluster (Optional - for Kubernetes)

k3d is separate from Docker Compose. To create a local Kubernetes cluster:

```bash
# Create k3d cluster
./scripts/setup-k3d.sh
```

Or manually:
```bash
k3d cluster create mlops-cluster \
  --port "8080:80@loadbalancer" \
  --port "8443:443@loadbalancer" \
  --port "5000:5000@loadbalancer" \
  --api-port 6443 \
  --servers 1 \
  --agents 1 \
  --wait
```

This creates a local Kubernetes cluster named `mlops-cluster` with:
- 1 server node
- 1 agent node
- LoadBalancer ports: 8080, 8443, 5000

### 5. Access Services

- **MinIO Console**: http://localhost:9001
  - Username: `minioadmin`
  - Password: `minioadmin`

- **Airflow UI**: http://localhost:8080
  - Username: `airflow`
  - Password: `airflow`

- **MinIO API**: http://localhost:9000

## Service Management

### Start Services
```bash
docker-compose up -d
```

### Stop Services
```bash
docker-compose down
```

### Stop and Remove Volumes (WARNING: deletes all data)
```bash
docker-compose down -v
```

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f minio
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler
```

### Check Service Status
```bash
docker-compose ps
```

## k3d Cluster Management

### Create Cluster
```bash
./scripts/setup-k3d.sh
```

### Delete Cluster
```bash
k3d cluster delete mlops-cluster
```

### List Clusters
```bash
k3d cluster list
```

### Use Cluster Context
```bash
kubectl config use-context k3d-mlops-cluster
```

### Check Cluster Status
```bash
kubectl get nodes
kubectl get namespaces
```

## Airflow DAGs

Place your DAGs in the `airflow/dags/` directory. They will be automatically loaded by Airflow.

Example DAG is provided at `airflow/dags/example_dag.py`.

## Environment Variables

Create a `.env` file (already provided) with:

```bash
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
```

## Troubleshooting

### MinIO not accessible
- Check if container is running: `docker-compose ps`
- Check logs: `docker-compose logs minio`
- Verify ports 9000 and 9001 are not in use

### Airflow not starting
- Check PostgreSQL is healthy: `docker-compose ps postgres`
- Check logs: `docker-compose logs airflow-webserver`
- Ensure AIRFLOW_UID is set correctly

### k3d cluster creation fails
- Ensure Docker is running
- Check available resources (memory, CPU)
- Try deleting existing cluster first: `k3d cluster delete mlops-cluster`

### DVC push/pull fails
- Verify MinIO bucket exists: `mc ls myminio/`
- Check DVC remote config: `dvc remote list`
- Ensure MinIO is accessible: `curl http://localhost:9000/minio/health/live`

## Next Steps

1. Configure your DVC pipeline to use MinIO
2. Create Airflow DAGs for your MLOps workflow
3. Deploy your Flask app to k3d cluster
4. Set up monitoring with Prometheus and Grafana (optional)

## Cleanup

To remove all services and data:

```bash
# Stop and remove Docker containers and volumes
docker-compose down -v

# Delete k3d cluster (if created)
k3d cluster delete mlops-cluster
```
