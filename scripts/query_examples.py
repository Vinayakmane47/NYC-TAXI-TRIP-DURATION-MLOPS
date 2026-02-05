#!/usr/bin/env python3
"""
Example Python script to query Gold layer tables from PostgreSQL
"""
import pandas as pd
from sqlalchemy import create_engine

# Create database connection
engine = create_engine('postgresql://airflow:airflow@localhost:5432/airflow')

print("=" * 80)
print("Querying NYC Taxi Gold Layer Data from PostgreSQL")
print("=" * 80)

# Query 1: Daily trip summary for January 2025
print("\n1. Daily Trip Summary (January 2025):")
query1 = """
SELECT
    pickup_day_of_month as day,
    trip_count,
    ROUND(avg_duration_min::numeric, 2) as avg_duration,
    ROUND(avg_distance::numeric, 2) as avg_distance,
    ROUND(total_revenue::numeric, 2) as revenue
FROM gold_daily_trip_summary
WHERE pickup_year = 2025 AND pickup_month = 1
ORDER BY pickup_day_of_month
LIMIT 10;
"""
df1 = pd.read_sql(query1, engine)
print(df1.to_string(index=False))

# Query 2: Rush hour patterns
print("\n2. Rush Hour vs Non-Rush Hour:")
query2 = """
SELECT
    is_rush_hour,
    SUM(trip_count) as total_trips,
    ROUND(AVG(avg_duration_min)::numeric, 2) as avg_duration,
    ROUND(AVG(avg_fare_per_mile)::numeric, 3) as avg_fare_per_mile
FROM gold_hourly_patterns
WHERE is_weekend = false
GROUP BY is_rush_hour;
"""
df2 = pd.read_sql(query2, engine)
print(df2.to_string(index=False))

# Query 3: Top 10 pickup locations
print("\n3. Top 10 Busiest Pickup Locations:")
query3 = """
SELECT
    pulocationid as location_id,
    pickup_count,
    ROUND(avg_distance_from_zone::numeric, 2) as avg_distance,
    ROUND(avg_fare_from_zone::numeric, 2) as avg_fare
FROM gold_pickup_location_stats
ORDER BY pickup_count DESC
LIMIT 10;
"""
df3 = pd.read_sql(query3, engine)
print(df3.to_string(index=False))

# Query 4: Weekend vs Weekday comparison
print("\n4. Weekend vs Weekday Patterns:")
query4 = """
SELECT
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    SUM(trip_count) as total_trips,
    ROUND(AVG(avg_duration_min)::numeric, 2) as avg_duration
FROM gold_hourly_patterns
GROUP BY is_weekend
ORDER BY is_weekend;
"""
df4 = pd.read_sql(query4, engine)
print(df4.to_string(index=False))

# Optional: Export to CSV
print("\n" + "=" * 80)
print("Exporting results to CSV files...")
df1.to_csv('daily_summary_jan2025.csv', index=False)
df2.to_csv('rush_hour_analysis.csv', index=False)
df3.to_csv('top_pickup_locations.csv', index=False)
df4.to_csv('weekend_vs_weekday.csv', index=False)
print("✓ CSV files created in current directory")

print("=" * 80)
