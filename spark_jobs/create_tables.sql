-- ============================================================================
-- Spark SQL Table Definitions for NYC Taxi Trip Duration MLOps Project
-- ============================================================================
-- These tables expose Parquet files in MinIO as SQL tables for BI tools
-- ============================================================================

-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS nyc_taxi;

USE nyc_taxi;

-- ============================================================================
-- Bronze Layer Table (Raw Data)
-- ============================================================================
DROP TABLE IF EXISTS bronze_trips;

CREATE EXTERNAL TABLE bronze_trips (
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    RatecodeID BIGINT,
    store_and_fwd_flag STRING,
    PULocationID STRING,
    DOLocationID STRING,
    payment_type BIGINT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    Airport_fee DOUBLE,
    cbd_congestion_fee DOUBLE
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3a://nyc-taxi-bronze/raw/bronze/'
TBLPROPERTIES (
    'spark.sql.sources.provider' = 'parquet',
    'spark.sql.partitionProvider' = 'catalog'
);

-- ============================================================================
-- Silver Layer Table (Cleaned Data)
-- ============================================================================
DROP TABLE IF EXISTS silver_trips;

CREATE EXTERNAL TABLE silver_trips (
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    RatecodeID BIGINT,
    store_and_fwd_flag STRING,
    PULocationID STRING,
    DOLocationID STRING,
    payment_type BIGINT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    Airport_fee DOUBLE,
    cbd_congestion_fee DOUBLE,
    trip_duration_min DOUBLE,
    route_id STRING
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3a://nyc-taxi-silver/silver/'
TBLPROPERTIES (
    'spark.sql.sources.provider' = 'parquet',
    'spark.sql.partitionProvider' = 'catalog'
);

-- ============================================================================
-- Gold Layer Table (Feature-Engineered Data)
-- ============================================================================
DROP TABLE IF EXISTS gold_trips;

CREATE EXTERNAL TABLE gold_trips (
    -- Original columns
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    RatecodeID BIGINT,
    store_and_fwd_flag STRING,
    PULocationID STRING,
    DOLocationID STRING,
    payment_type BIGINT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    Airport_fee DOUBLE,
    cbd_congestion_fee DOUBLE,
    trip_duration_min DOUBLE,
    route_id STRING,
    -- Time features
    pickup_hour INT,
    pickup_day_of_week INT,
    pickup_day_of_month INT,
    pickup_week_of_year INT,
    pickup_month INT,
    pickup_year INT,
    -- Boolean flags
    is_weekend BOOLEAN,
    is_peak_hour BOOLEAN,
    is_night BOOLEAN,
    is_rush_hour BOOLEAN,
    -- Cyclical encoding
    sin_hour DOUBLE,
    cos_hour DOUBLE,
    sin_day_of_week DOUBLE,
    cos_day_of_week DOUBLE,
    -- Distance features
    is_short_trip BOOLEAN,
    is_long_trip BOOLEAN,
    log_trip_distance DOUBLE,
    sqrt_trip_distance DOUBLE,
    -- Ratio features
    fare_per_mile DOUBLE,
    surcharge_ratio DOUBLE,
    toll_ratio DOUBLE,
    distance_per_minute DOUBLE,
    -- Flag features
    has_tolls INT,
    has_airport_fee INT,
    has_congestion_fee INT,
    non_fare_amount DOUBLE,
    -- Location features
    is_same_zone BOOLEAN,
    pickup_zone_trip_count BIGINT,
    dropoff_zone_trip_count BIGINT,
    route_trip_count BIGINT
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3a://nyc-taxi-gold/gold/'
TBLPROPERTIES (
    'spark.sql.sources.provider' = 'parquet',
    'spark.sql.partitionProvider' = 'catalog'
);

-- ============================================================================
-- Refresh partition metadata (run after data is loaded)
-- ============================================================================
-- MSCK REPAIR TABLE bronze_trips;
-- MSCK REPAIR TABLE silver_trips;
-- MSCK REPAIR TABLE gold_trips;

-- ============================================================================
-- Sample queries for verification
-- ============================================================================
-- SELECT COUNT(*) FROM bronze_trips WHERE year = 2025 AND month = 1;
-- SELECT COUNT(*) FROM silver_trips WHERE year = 2025 AND month = 1;
-- SELECT COUNT(*) FROM gold_trips WHERE year = 2025 AND month = 1;
