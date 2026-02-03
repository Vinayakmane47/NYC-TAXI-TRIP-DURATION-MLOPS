# Production MLOps Pipeline - Setup Guide

## Architecture Overview

This project follows production MLOps best practices with **separation of concerns**:

```
┌─────────────────────────────────────────────────────────────────┐
│                   Airflow (Orchestration Layer)                  │
│  • Schedules workflows                                           │
│  • Monitors job execution                                        │
│  • Manages dependencies                                          │
│  • NO heavy computation                                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Submits Jobs via SparkSubmitOperator
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Spark Cluster (Compute Layer)                   │
│  • Master node coordinates                                       │
│  • Worker nodes execute tasks                                    │
│  • Runs PySpark transformations                                  │
│  • Scales independently                                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Reads/Writes
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    MinIO (Storage Layer)                         │
│  • Bronze: Raw data                                              │
│  • Silver: Cleaned data                                          │
│  • Gold: Feature-engineered data                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Why This Architecture?

✅ **Scalability**: Spark cluster can scale independently from Airflow
✅ **Resource Efficiency**: Airflow stays lightweight, Spark handles heavy lifting
✅ **Production-Ready**: Industry-standard pattern used by companies like Uber, Netflix
✅ **Maintainability**: Clear separation makes debugging easier
✅ **Cost-Effective**: Only run Spark when needed, not 24/7

## Quick Start

### 1. Build and Start All Services

```bash
# Build custom images for Airflow and Spark
docker compose build

# Start all services
docker compose up -d
```

This starts:
- **MinIO**: S3-compatible storage (ports 9000, 9001)
- **Spark Master**: Coordinates Spark jobs (port 8081)
- **Spark Worker**: Executes Spark tasks (port 8082)
- **Airflow**: Webserver, Scheduler, Worker (port 8080)
- **Postgres**: Airflow metadata database
- **Redis**: Airflow task queue

### 2. Configure Spark Connection in Airflow

```bash
# Run the setup script
./scripts/setup_airflow_connections.sh
```

Or manually in Airflow UI:
1. Go to http://localhost:8080
2. Admin → Connections → Add Connection
3. Configure:
   - **Conn ID**: `spark_default`
   - **Conn Type**: `Spark`
   - **Host**: `spark-master`
   - **Port**: `7077`

### 3. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| Spark Master UI | http://localhost:8081 | No auth |
| Spark Worker UI | http://localhost:8082 | No auth |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

### 4. Run the ETL Pipeline

1. Open Airflow UI: http://localhost:8080
2. Find DAG: `nyc_taxi_etl_production`
3. Enable it (toggle switch)
4. Click "Play" to trigger

**Pipeline Flow:**
```
Data Ingestion (Airflow Worker)
    ↓
Data Preprocessing (Spark Cluster)
    ↓
Feature Engineering (Spark Cluster)
    ↓
Done!
```

## How It Works

### 1. Data Ingestion
- **Runs on**: Airflow Worker (no Spark needed)
- **What it does**: Downloads taxi data from NYC API
- **Output**: Raw parquet files in MinIO bronze bucket

### 2. Data Preprocessing
- **Runs on**: Spark Cluster
- **Job file**: `spark_jobs/data_preprocessing_job.py`
- **What it does**:
  - Cleans data (removes outliers, invalid records)
  - Handles missing values
  - Filters based on quality rules
- **Output**: Cleaned parquet files in MinIO silver bucket

### 3. Feature Engineering
- **Runs on**: Spark Cluster
- **Job file**: `spark_jobs/feature_engineering_job.py`
- **What it does**:
  - Creates time-based features (hour, day, weekend)
  - Calculates distance features
  - Adds ratio features (fare per mile)
- **Output**: Feature-rich parquet in MinIO gold bucket

## Monitoring

### Spark Job Execution
- Visit http://localhost:8081 to see Spark Master UI
- Monitor running applications, resource usage
- Check worker status

### Airflow DAG Status
- Visit http://localhost:8080
- View task logs, execution history
- Check for failures and retries

## Development Workflow

### Testing Spark Jobs Locally

```bash
# Submit a job directly to Spark (bypass Airflow)
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/airflow/spark_jobs/data_preprocessing_job.py \
  --year 2025
```

### Modifying Spark Jobs

1. Edit files in `spark_jobs/`
2. No need to rebuild - volumes are mounted
3. Just re-run the DAG in Airflow

### Adding New Features

1. Update transformation logic in `src/data_transformation/`
2. Modify Spark job scripts in `spark_jobs/`
3. Update DAG in `airflow/dags/` if needed

## Scaling

### Add More Spark Workers

Edit `docker-compose.yml`:

```yaml
spark-worker-2:
  image: nyc-taxi-spark:latest
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
  # ... same config as spark-worker
```

### Increase Worker Resources

```yaml
spark-worker:
  environment:
    - SPARK_WORKER_MEMORY=8G  # Increase from 4G
    - SPARK_WORKER_CORES=4     # Increase from 2
```

## Troubleshooting

### Spark Job Fails

1. Check Spark Master UI: http://localhost:8081
2. Look for failed applications
3. Click on app → Executor → stderr for errors

### Airflow Can't Connect to Spark

```bash
# Verify Spark Master is running
docker logs spark-master

# Test connection from Airflow
docker exec airflow-webserver nc -zv spark-master 7077
```

### Out of Memory

Increase Spark executor memory in DAG:

```python
conf={
    'spark.executor.memory': '4g',  # Increase from 2g
    'spark.driver.memory': '2g',
}
```

## Production Considerations

### For Real Production Deployment:

1. **Use Managed Spark**: AWS EMR, Databricks, Google Dataproc
2. **Secure Credentials**: Use secrets manager, not hardcoded
3. **Monitoring**: Add DataDog, Prometheus metrics
4. **Alerts**: Configure email/Slack alerts in Airflow
5. **Resource Limits**: Set proper CPU/memory limits
6. **Data Retention**: Implement lifecycle policies in S3/MinIO
7. **Backup**: Regular backups of Airflow metadata DB

## Key Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Defines all services |
| `Dockerfile.airflow` | Lightweight Airflow image |
| `Dockerfile.spark` | Spark with project dependencies |
| `airflow/dags/nyc_taxi_etl_production_dag.py` | Main ETL DAG |
| `spark_jobs/*.py` | Standalone Spark applications |
| `src/data_transformation/` | Reusable PySpark transformations |

## Benefits Over All-in-One Approach

| Aspect | All-in-One (Airflow + PySpark) | Production (Separate Spark) |
|--------|-------------------------------|----------------------------|
| Scalability | Limited | Excellent |
| Resource Usage | High (always running) | Efficient (on-demand) |
| Debugging | Hard (mixed logs) | Easy (separate layers) |
| Cost | Higher | Lower |
| Industry Standard | No | Yes |
| Production Ready | No | Yes |

## Next Steps

1. ✅ Pipeline is running!
2. Monitor first execution
3. Verify data in MinIO buckets
4. Add model training stage (separate Spark job)
5. Implement model serving
6. Set up CI/CD

---

**Questions?** Check Spark UI logs and Airflow task logs for detailed information!
