# Fix Java 21 Compatibility Issue

## The Problem
Spark 3.4.1 requires Java 17, but your Docker images use Java 21, causing all Spark jobs to fail with:
```
java.lang.NoSuchMethodException: java.nio.DirectByteBuffer.<init>(long,int)
```

## The Solution

### 1. Update Dockerfile.spark

Find this line in `Dockerfile.spark`:
```dockerfile
RUN apt-get install -y openjdk-21-jre-headless
```

Change it to:
```dockerfile
RUN apt-get install -y openjdk-17-jre-headless
```

### 2. Rebuild and Restart

```bash
# Rebuild Spark images
docker compose build spark-master spark-worker

# Restart all services
docker compose down
docker compose up -d
```

### 3. Verify

```bash
# Check Java version in Spark
docker exec spark-master java -version
# Should show: openjdk version "17.x.x"
```

### 4. Run Export

Once fixed, run:
```bash
bash scripts/export_to_superset.sh
```

## Impact

Fixing this will:
- ✅ Fix ALL Spark jobs (ETL pipeline, exports, etc.)
- ✅ Allow Thrift Server to work (optional)
- ✅ Enable proper Superset integration
- ⏱️ Takes ~5-10 minutes to rebuild

This is the PROPER solution and fixes everything permanently.
