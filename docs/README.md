# NYC Taxi Trip Duration Prediction - MLOps Pipeline

A production-ready pipeline for predicting NYC taxi trip durations using machine learning and automated data processing.

---

## What This Project Does

This project automatically:
1. **Downloads** taxi trip data from NYC's public database
2. **Cleans** the data (removes errors, fills missing values)
3. **Creates features** (adds useful information like time of day, distance calculations)
4. **Prepares data** for machine learning models

Everything runs automatically on a schedule using Airflow.

---

## Project Structure

```
NYC-TAXI-TRIP-DURATION-MLOPS/
│
├── airflow/               # Automation (runs tasks on schedule)
│   └── dags/             # Task definitions
│
├── src/                  # Main code
│   ├── data_ingestion/   # Downloads data
│   ├── data_transformation/  # Cleans and processes data
│   ├── data_validation/  # Checks data quality
│   ├── feature_store/    # Stores features for ML
│   ├── config/          # Settings and parameters
│   ├── logger/          # Logging system
│   └── utils/           # Helper functions
│
├── data/                # Downloaded data goes here
│   ├── raw/bronze/      # Original downloaded data
│   ├── processed/silver/  # Cleaned data
│   └── features/gold/   # Final data ready for ML
│
├── scripts/             # Helper scripts
└── docker-compose.yml   # Setup file for services
```

---

## How It Works

### Step 1: Data Download (Bronze Layer)
- Downloads NYC yellow taxi trip data
- Organizes files by year and month
- Stores in `data/raw/bronze/`

### Step 2: Data Cleaning (Silver Layer)
- Removes invalid trips (negative distances, impossible durations)
- Fills in missing values
- Filters outliers
- Stores cleaned data in `data/processed/silver/`

### Step 3: Feature Creation (Gold Layer)
- Adds time features (hour of day, day of week, weekend flag)
- Adds distance features (log distance, short/long trip flags)
- Creates ratios (fare per mile, surcharge ratio)
- Stores final data in `data/features/gold/`

### Step 4: Automation (Airflow)
- Runs all steps automatically
- Can schedule daily, weekly, or on-demand
- Sends alerts if something fails

---

## Quick Start (One Command!)

### Start Everything
```bash
docker compose up -d
```

That's it! This single command:
- Starts Airflow (workflow automation)
- Starts MinIO (S3-compatible storage)
- Creates all necessary buckets
- Sets up the database
- Initializes the scheduler

### Access the Services

**Airflow UI**: http://localhost:8080
- Username: `admin`
- Password: `admin`

**Apache Superset (BI Dashboards)**: http://localhost:8088
- Username: `admin`
- Password: `admin`
- See [Business Intelligence](#business-intelligence) section below

**MinIO Console**: http://localhost:9001
- Username: `minioadmin`
- Password: `minioadmin`

**Spark Master UI**: http://localhost:8081
- Monitor Spark jobs and cluster status

**Spark Worker UI**: http://localhost:8082
- Monitor Spark worker nodes

### Run the ETL Pipeline

1. Open Airflow UI: http://localhost:8080
2. Find `nyc_taxi_etl_pipeline` in the list
3. Toggle the switch to enable it
4. Click the "Play" button to trigger it

Watch it run! The pipeline will:
- Download taxi data > Clean it > Create features > Save results

### Stop Everything
```bash
docker compose down
```

---

## Business Intelligence with Apache Superset

This project includes Apache Superset for creating interactive dashboards and analytics on your NYC taxi trip data.

### Accessing Superset

1. **Start all services** (if not already running):
   ```bash
   docker compose up -d
   ```

2. **Access Superset UI**: http://localhost:8088
   - Username: `admin`
   - Password: `admin`

### Setting Up Data Source Connection

1. **Login to Superset** at http://localhost:8088

2. **Add Database Connection**:
   - Go to **Settings** → **Database Connections** → **+ Database**
   - Select **Spark SQL** as the database type
   - **Connection String**: `hive://spark-thrift-server:10000/nyc_taxi`
   - **Display Name**: `NYC Taxi Data`
   - Click **Test Connection** to verify
   - Click **Connect**

### Creating Tables in Spark SQL

Before querying data, you need to create the SQL tables. Run the initialization script:

```bash
# Execute inside the spark-thrift-server container
docker exec -it spark-thrift-server bash /opt/airflow/scripts/init_spark_tables.sh
```

Or manually via Spark SQL:
```bash
docker exec -it spark-thrift-server spark-sql \
  --master spark://spark-master:7077 \
  -f /opt/airflow/spark_jobs/create_tables.sql
```

### Available Tables

After initialization, you'll have access to three tables:

1. **`bronze_trips`** - Raw data from NYC TLC
   - Location: `s3a://nyc-taxi-bronze/raw/bronze/`
   - Partitioned by: `year`, `month`

2. **`silver_trips`** - Cleaned and validated data
   - Location: `s3a://nyc-taxi-silver/silver/`
   - Partitioned by: `year`, `month`
   - Includes: `trip_duration_min`, `route_id`

3. **`gold_trips`** - Feature-engineered data ready for ML
   - Location: `s3a://nyc-taxi-gold/gold/`
   - Partitioned by: `year`, `month`
   - Includes: Time features, cyclical encodings, distance features, ratios, location features

### Sample Queries for Dashboards

#### 1. Trip Volume by Hour
```sql
SELECT 
    pickup_hour,
    COUNT(*) as trip_count
FROM gold_trips
WHERE year = 2025
GROUP BY pickup_hour
ORDER BY pickup_hour;
```

#### 2. Average Trip Duration by Day of Week
```sql
SELECT 
    pickup_day_of_week,
    AVG(trip_duration_min) as avg_duration_min,
    COUNT(*) as trip_count
FROM gold_trips
WHERE year = 2025
GROUP BY pickup_day_of_week
ORDER BY pickup_day_of_week;
```

#### 3. Revenue Analysis by Month
```sql
SELECT 
    pickup_month,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_fare,
    COUNT(*) as trip_count
FROM gold_trips
WHERE year = 2025
GROUP BY pickup_month
ORDER BY pickup_month;
```

#### 4. Distance vs Duration Correlation
```sql
SELECT 
    CASE 
        WHEN trip_distance < 1 THEN 'Short (<1mi)'
        WHEN trip_distance < 5 THEN 'Medium (1-5mi)'
        WHEN trip_distance < 10 THEN 'Long (5-10mi)'
        ELSE 'Very Long (>10mi)'
    END as distance_category,
    AVG(trip_duration_min) as avg_duration,
    AVG(trip_distance) as avg_distance,
    COUNT(*) as trip_count
FROM gold_trips
WHERE year = 2025
GROUP BY distance_category;
```

#### 5. Popular Pickup Locations
```sql
SELECT 
    PULocationID,
    COUNT(*) as pickup_count,
    AVG(trip_duration_min) as avg_duration
FROM gold_trips
WHERE year = 2025
GROUP BY PULocationID
ORDER BY pickup_count DESC
LIMIT 20;
```

### Creating Your First Dashboard

1. **Create a Chart**:
   - Go to **Charts** → **+ Chart**
   - Select your database connection
   - Choose a table (e.g., `gold_trips`)
   - Select chart type (e.g., Bar Chart, Line Chart, Table)
   - Write your SQL query
   - Click **Run** to preview
   - Click **Save** to save the chart

2. **Create a Dashboard**:
   - Go to **Dashboards** → **+ Dashboard**
   - Name your dashboard (e.g., "NYC Taxi Analytics")
   - Click **Save**
   - Click **Edit Dashboard**
   - Add your saved charts to the dashboard
   - Arrange and resize charts as needed
   - Click **Save**

### Troubleshooting Superset

**Connection Issues**:
- Verify Spark Thrift Server is running: `docker ps | grep spark-thrift-server`
- Check Thrift Server logs: `docker logs spark-thrift-server`
- Ensure tables are created (run initialization script)

**Query Performance**:
- Use partitioned columns (`year`, `month`) in WHERE clauses for faster queries
- Limit result sets with `LIMIT` clause
- Consider creating materialized views for frequently accessed aggregations

**Tables Not Visible**:
- Run the table initialization script
- Refresh the database connection in Superset
- Verify data exists in MinIO buckets

---

## Configuration

All settings are in `src/config/` as YAML files:

### Change Download Year
Edit `src/config/ingestion.yaml`:
```yaml
year: 2024  # Change to desired year
```

### Adjust Data Quality Rules
Edit `src/config/preprocessing.yaml`:
```yaml
data_cleaning:
  max_trip_duration_min: 240  # Allow 4-hour trips instead of 3
  max_trip_distance: 100      # Allow 100-mile trips
```

### Add/Remove Features
Edit `src/config/feature_engineering.yaml`:
```yaml
feature_engineering:
  include_time_features: true
  include_distance_features: true
  include_ratio_features: false  # Disable ratio features
```

---

## Data Flow

```
NYC TLC API
    |
    v (Download)
data/raw/bronze/2023/01/
    |
    v (Clean)
data/processed/silver/2023/
    |
    v (Add Features)
data/features/gold/2023/
    |
    v (Ready for ML)
Model Training
```

---

## Logs

Check logs for debugging:
- Application logs: `logs/nyc_taxi_mlops.log`
- Airflow logs: `airflow/logs/`

---

## Troubleshooting

### Data not downloading?
- Check internet connection
- Verify year is valid (NYC data available from 2019 onwards)
- Check logs in `logs/nyc_taxi_mlops.log`

### Processing fails?
- Make sure data exists in bronze layer first
- Check Spark has enough memory
- Review logs for specific errors

### Airflow not starting?
```bash
cd airflow
docker-compose down
docker-compose up -d
```

---

## Project Status

**Complete and Working**:
- Data ingestion
- Data cleaning
- Feature engineering
- Data validation
- Airflow automation

**Complete and Working**:
- Data ingestion
- Data cleaning
- Feature engineering
- Data validation
- Airflow automation
- **Business Intelligence (Superset)** ✨ NEW

**Planned for Later**:
- Model training
- Model evaluation
- Model deployment
- Real-time predictions
