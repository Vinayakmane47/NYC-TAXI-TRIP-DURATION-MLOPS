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

**MinIO UI**: http://localhost:9000 (optional)
- Username: `minioadmin`
- Password: `minioadmin`

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

**Planned for Later**:
- Model training
- Model evaluation
- Model deployment
- Real-time predictions
