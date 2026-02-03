"""
Feast Feature Store Definitions for NYC Taxi Trip Duration Project
"""

from datetime import timedelta
from feast import Entity, Feature, FeatureView, ValueType
from feast.data_source import FileSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Int64, String

# Define entities
trip_entity = Entity(
    name="trip_id",
    value_type=ValueType.INT64,
    description="Unique trip identifier",
)

location_entity = Entity(
    name="location_pair",
    value_type=ValueType.STRING,
    description="Pickup and dropoff location pair (PULocationID_DOLocationID)",
)

# Define data source for gold layer features
gold_features_source = FileSource(
    name="gold_features",
    path="s3a://nyc-taxi-gold/gold/",  # Will be configured dynamically
    timestamp_field="tpep_pickup_datetime",
    created_timestamp_column="created_at",
)

# Feature View: Trip Features
trip_features = FeatureView(
    name="trip_features",
    entities=[trip_entity, location_entity],
    ttl=timedelta(days=365),
    features=[
        Feature(name="trip_duration_min", dtype=Float32),
        Feature(name="trip_distance", dtype=Float32),
        Feature(name="passenger_count", dtype=Int64),
        Feature(name="fare_amount", dtype=Float32),
        Feature(name="tip_amount", dtype=Float32),
        Feature(name="total_amount", dtype=Float32),
        Feature(name="hour_of_day", dtype=Int64),
        Feature(name="day_of_week", dtype=Int64),
        Feature(name="month", dtype=Int64),
        Feature(name="sin_hour", dtype=Float32),
        Feature(name="cos_hour", dtype=Float32),
        Feature(name="sin_day_of_week", dtype=Float32),
        Feature(name="cos_day_of_week", dtype=Float32),
        Feature(name="distance_per_minute", dtype=Float32),
        Feature(name="fare_per_mile", dtype=Float32),
    ],
    source=gold_features_source,
    tags={"team": "mlops", "domain": "taxi"},
)
