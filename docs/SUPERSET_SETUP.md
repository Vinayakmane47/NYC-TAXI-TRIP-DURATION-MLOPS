# Apache Superset Setup Guide

This guide provides detailed instructions for setting up and using Apache Superset for Business Intelligence on NYC Taxi Trip Duration data.

## IMPORTANT: Current Setup (Java 21 Issue)

Due to Java 21 compatibility issues with Spark Thrift Server, we're using **PostgreSQL as the bridge** between Superset and your data lakehouse.

## Architecture Overview

```
+------------------+
| Apache Superset  |  Port 8088 (UI)
|   (BI Tool)      |
+--------+---------+
         | Direct SQL Connection
         v
+------------------+
|  PostgreSQL      |  Port 5432
|  (airflow db)    |  Contains: gold_* tables
+--------+---------+
         | Populated by
         v
+------------------+        +---------------+
|  Spark Jobs      |------->|  MinIO S3     |
|  (ETL + Export)  | Reads  |  Parquet      |
+------------------+        +---------------+
```

**How it works:**
1. Your Spark jobs process data and store it as Parquet in MinIO (Bronze/Silver/Gold)
2. An export job reads from Gold layer and writes aggregated views to PostgreSQL
3. Superset queries PostgreSQL directly (fast and reliable)

## Prerequisites

- Docker and Docker Compose installed
- All services running (`docker compose up -d`)
- Data processed through the ETL pipeline (bronze, silver, gold layers)

## Initial Setup

### 1. Start All Services

```bash
docker compose up -d
```

This starts:
- Superset (port 8088)
- Spark SQL Thrift Server (port 10000)
- Spark Master/Worker
- MinIO
- PostgreSQL (for Superset metadata)

### 2. Initialize Spark SQL Tables

The tables need to be created in Spark SQL before Superset can query them:

```bash
# Option 1: Using the initialization script
docker exec -it spark-thrift-server bash /opt/airflow/scripts/init_spark_tables.sh

# Option 2: Manual execution via Spark SQL
docker exec -it spark-thrift-server spark-sql \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  -f /opt/airflow/spark_jobs/create_tables.sql
```

### 3. Verify Tables Created

```bash
# Connect to Spark SQL
docker exec -it spark-thrift-server spark-sql \
  --master spark://spark-master:7077

# Then run:
SHOW DATABASES;
USE nyc_taxi;
SHOW TABLES;
```

You should see:
- `bronze_trips`
- `silver_trips`
- `gold_trips`

## Superset Configuration

### 1. Access Superset UI

Navigate to: http://localhost:8088

Default credentials:
- Username: `admin`
- Password: `admin`

### 2. Add Database Connection

**Note**: Due to Java 21 compatibility issues with Spark Thrift Server, you may need to use an alternative connection method. See Troubleshooting section below.

**Method 1: Direct Spark Connection (If Thrift Server is not working)**

1. Click **Settings** (gear icon) -> **Database Connections**
2. Click **+ Database** button
3. Select **Other** from the dropdown
4. Fill in connection details:
   - **Display Name**: `NYC Taxi Data (Spark)`
   - **SQLAlchemy URI**: `spark://spark-master:7077`
   - Note: This may require additional configuration

**Method 2: Spark SQL via Thrift Server (If working)**

1. Click **Settings** (gear icon) -> **Database Connections**
2. Click **+ Database** button
3. Select **Spark SQL** from the dropdown
4. Fill in connection details:
   - **Display Name**: `NYC Taxi Data`
   - **SQLAlchemy URI**: `hive://spark-thrift-server:10000/nyc_taxi`
   - **Database Name**: `nyc_taxi`
5. Click **Test Connection**
6. If successful, click **Connect**

### Alternative: Using JDBC Connection

If Spark SQL connection doesn't work, try JDBC:

1. Select **Other** as database type
2. **SQLAlchemy URI**: `jdbc:hive2://spark-thrift-server:10000/nyc_taxi`
3. You may need to install Hive JDBC driver

## Creating Charts

### Example 1: Trip Volume by Hour

1. Go to **Charts** -> **+ Chart**
2. Select **NYC Taxi Data** database
3. Select **gold_trips** table
4. Choose **Bar Chart** as visualization type
5. Enter SQL:

```sql
SELECT 
    pickup_hour,
    COUNT(*) as trip_count
FROM gold_trips
WHERE year = 2025 AND month = 1
GROUP BY pickup_hour
ORDER BY pickup_hour
```

6. Configure chart:
   - **X Axis**: `pickup_hour`
   - **Y Axis**: `trip_count`
7. Click **Run** to preview
8. Click **Save** -> Name it "Trip Volume by Hour"

### Example 2: Revenue by Month

1. Create new chart
2. Choose **Line Chart**
3. SQL:

```sql
SELECT 
    pickup_month,
    SUM(total_amount) as total_revenue,
    COUNT(*) as trip_count
FROM gold_trips
WHERE year = 2025
GROUP BY pickup_month
ORDER BY pickup_month
```

4. Configure:
   - **X Axis**: `pickup_month`
   - **Y Axis**: `total_revenue`
5. Save as "Revenue by Month"

### Example 3: Average Duration by Day of Week

1. Create new chart
2. Choose **Bar Chart**
3. SQL:

```sql
SELECT 
    CASE pickup_day_of_week
        WHEN 0 THEN 'Monday'
        WHEN 1 THEN 'Tuesday'
        WHEN 2 THEN 'Wednesday'
        WHEN 3 THEN 'Thursday'
        WHEN 4 THEN 'Friday'
        WHEN 5 THEN 'Saturday'
        WHEN 6 THEN 'Sunday'
    END as day_name,
    AVG(trip_duration_min) as avg_duration
FROM gold_trips
WHERE year = 2025
GROUP BY pickup_day_of_week
ORDER BY pickup_day_of_week
```

4. Configure and save

## Creating Dashboards

1. Go to **Dashboards** -> **+ Dashboard**
2. Name: "NYC Taxi Analytics"
3. Click **Save**
4. Click **Edit Dashboard**
5. Click **+ Add Chart** and select your saved charts
6. Arrange charts by dragging and resizing
7. Click **Save**

## Performance Optimization

### Use Partition Pruning

Always include partition columns in WHERE clauses:

```sql
-- Good: Uses partition pruning
SELECT * FROM gold_trips 
WHERE year = 2025 AND month = 1
LIMIT 100;

-- Bad: Scans all partitions
SELECT * FROM gold_trips 
LIMIT 100;
```

### Limit Result Sets

```sql
-- Always use LIMIT for large tables
SELECT * FROM gold_trips 
WHERE year = 2025 
LIMIT 1000;
```

### Create Materialized Views (Advanced)

For frequently accessed aggregations, consider creating views:

```sql
CREATE VIEW IF NOT EXISTS daily_trip_summary AS
SELECT 
    pickup_year,
    pickup_month,
    pickup_day_of_month,
    COUNT(*) as trip_count,
    AVG(trip_duration_min) as avg_duration,
    SUM(total_amount) as total_revenue
FROM gold_trips
GROUP BY pickup_year, pickup_month, pickup_day_of_month;
```

## Troubleshooting

### Spark Thrift Server Java Compatibility Issue

**Problem**: Spark Thrift Server fails to start with error:
```
java.lang.NoSuchMethodException: java.nio.DirectByteBuffer.<init>(long,int)
```

**Cause**: Spark 3.4.1 has compatibility issues with Java 21. The current Docker image uses Java 21.

**Solutions**:

1. **Option 1: Use Spark SQL directly (Recommended for now)**
   - Connect to Spark SQL using the Spark cluster directly
   - Use Superset's Spark SQL connector pointing to the Spark master
   - Connection: `spark://spark-master:7077`

2. **Option 2: Rebuild with Java 17**
   - Modify `Dockerfile.spark` to use Java 17 instead of Java 21
   - Change `openjdk-21-jre-headless` to `openjdk-17-jre-headless`
   - Rebuild: `docker compose build spark-master spark-worker spark-thrift-server`

3. **Option 3: Use alternative BI connection**
   - Export data to PostgreSQL/MySQL and connect Superset to that
   - Use Spark's JDBC server on a different port

**Note**: The Spark Thrift Server service may not start due to this compatibility issue. The Spark cluster itself (master/worker) works fine for ETL jobs. For BI access, consider using Option 1 or 2 above.

### Connection Refused

**Problem**: Cannot connect to Spark Thrift Server

**Solution**:
```bash
# Check if service is running
docker ps | grep spark-thrift-server

# Check logs
docker logs spark-thrift-server

# Restart service
docker restart spark-thrift-server
```

### Tables Not Found

**Problem**: Tables don't appear in Superset

**Solution**:
1. Verify tables exist in Spark SQL:
   ```bash
   docker exec -it spark-thrift-server spark-sql \
     --master spark://spark-master:7077 \
     -e "USE nyc_taxi; SHOW TABLES;"
   ```

2. Refresh database connection in Superset
3. Re-run table creation script

### Slow Queries

**Problem**: Queries take too long

**Solutions**:
- Always filter by `year` and `month` partitions
- Use `LIMIT` to restrict result size
- Create indexes on frequently filtered columns (if supported)
- Consider pre-aggregating data for dashboards

### Authentication Issues

**Problem**: Cannot login to Superset

**Solution**:
```bash
# Reset admin password
docker exec -it superset superset fab reset-password \
  --username admin \
  --password newpassword
```

## Advanced Configuration

### Custom Superset Configuration

Edit `superset/config/superset_config.py` to customize:
- Cache settings
- Security settings
- Feature flags
- Timezone

### Adding More Data Sources

Superset supports many data sources. To add another:

1. Go to **Settings** -> **Database Connections**
2. Click **+ Database**
3. Select your database type
4. Configure connection string
5. Test and connect

## Sample Dashboard Queries

See `docs/README.md` for more sample queries including:
- Trip volume analysis
- Revenue trends
- Distance vs duration
- Location heatmaps
- Time-based patterns

## Next Steps

1. Create custom charts for your specific use cases
2. Build comprehensive dashboards
3. Set up alerts for data quality issues
4. Share dashboards with your team
5. Schedule regular data refreshes

## Resources

- [Superset Documentation](https://superset.apache.org/docs/)
- [Spark SQL Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [SQL Reference](https://spark.apache.org/docs/latest/sql-ref.html)
