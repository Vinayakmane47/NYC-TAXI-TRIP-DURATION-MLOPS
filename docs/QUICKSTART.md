# Quick Start Guide

## Start Everything

```bash
docker-compose up -d
```

That's it! This will automatically:
- ✅ Start MinIO and create `dvc-storage` bucket
- ✅ Start Airflow and initialize database
- ✅ Start PostgreSQL and Redis

## Access Services

- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **MinIO API**: http://localhost:9000
- **Airflow UI**: http://localhost:8080 (airflow/airflow)

## Common Commands

```bash
# Start services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down

# Stop and remove all data
docker-compose down -v

# Check status
docker-compose ps

# Restart a specific service
docker-compose restart minio
```

## Configure DVC with MinIO

After services are running:

```bash
dvc remote add -d minio s3://dvc-storage --endpoint-url http://localhost:9000
dvc remote modify minio endpointurl http://localhost:9000
dvc remote modify minio access_key_id minioadmin
dvc remote modify minio secret_access_key minioadmin
```

## k3d Setup (Optional - for Kubernetes)

k3d is separate from Docker Compose:

```bash
# Create cluster
./scripts/setup-k3d.sh

# Or manually
k3d cluster create mlops-cluster \
  --port "5000:5000@loadbalancer" \
  --servers 1 \
  --agents 1 \
  --wait
```
