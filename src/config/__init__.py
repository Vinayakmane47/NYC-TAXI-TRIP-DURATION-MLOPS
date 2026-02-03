## Create Configs for data ingestion , data preprocessing , feature engineering , model building , model evaluation , model registration

## Create Configs for data ingestion
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Dict
import os
import sys
import logging
import json
import yaml
import pandas as pd

@dataclass
class S3Config:
    """S3/MinIO storage configuration."""
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    region: str = "us-east-1"
    use_ssl: bool = False
    path_style_access: bool = True

@dataclass
class DataIngestionConfig:
    root_dir: Path
    api_base_url: str
    file_name_pattern: str
    year: int
    raw_bronze_dir: Path
    download_dir: Path
    chunk_size: int
    max_retries: int
    retry_delay: int
    validate_download: bool
    min_file_size_mb: float
    # S3/MinIO configuration
    s3: Optional[S3Config] = None
    bronze_bucket: Optional[str] = None
    use_s3: bool = False

## Create Configs for data preprocessing

@dataclass
class DataCleaningConfig:
    min_trip_duration_min: float
    max_trip_duration_min: float
    min_trip_distance: float
    max_trip_distance: float
    min_fare_amount: float
    max_fare_amount: float
    min_total_amount: float
    max_tip_multiplier: float

@dataclass
class SparkConfig:
    app_name: str
    master: str
    config: Dict

@dataclass
class ProcessingConfig:
    year: int
    months: List[int]
    repartition: bool
    num_partitions: int
    cache_intermediate: bool
    write_partitions: int = 50

@dataclass
class OutputConfig:
    format: str
    compression: str
    mode: str

@dataclass
class DataPreprocessingConfig:
    root_dir: Path
    raw_bronze_dir: Path
    silver_dir: Path
    artifact_dir: Path
    data_cleaning: DataCleaningConfig
    spark: SparkConfig
    processing: ProcessingConfig
    output: OutputConfig
    # S3/MinIO configuration
    s3: Optional[S3Config] = None
    bronze_bucket: Optional[str] = None
    silver_bucket: Optional[str] = None
    use_s3: bool = False


## Create Configs for feature engineering

@dataclass
class FeatureEngineeringSettings:
    include_time_features: bool
    include_cyclical_encoding: bool
    include_distance_features: bool
    include_ratio_features: bool
    include_location_features: bool
    compute_frequency_features: bool
    include_flag_features: bool

@dataclass
class FeatureEngineeringConfig:
    root_dir: Path
    silver_dir: Path
    gold_dir: Path
    artifact_dir: Path
    feature_engineering: FeatureEngineeringSettings
    spark: SparkConfig
    processing: ProcessingConfig
    output: OutputConfig
    # S3/MinIO configuration
    s3: Optional[S3Config] = None
    silver_bucket: Optional[str] = None
    gold_bucket: Optional[str] = None
    use_s3: bool = False

## Create Configs for model building
@dataclass
class ModelBuildingConfig:
    root_dir: Path
    data_path: Path
    artifact_dir: Path
    train_data_path: Path
    test_data_path: Path
    val_data_path: Path
    all_files: List[str]

## Create Configs for model evaluation
@dataclass
class ModelEvaluationConfig:
    root_dir: Path
    data_path: Path
    artifact_dir: Path
    train_data_path: Path
    test_data_path: Path
    val_data_path: Path
    all_files: List[str]

## Create Configs for model registration    
@dataclass
class ModelRegistrationConfig:
    root_dir: Path
    data_path: Path
    artifact_dir: Path
    train_data_path: Path
    test_data_path: Path
    val_data_path: Path
    all_files: List[str]

## Create Configs for data validation

@dataclass
class MissingValueThresholds:
    critical_columns: float
    important_columns: float
    optional_columns: float

@dataclass
class OutlierDetection:
    enabled: bool
    method: str
    zscore_threshold: float
    iqr_multiplier: float

@dataclass
class DuplicateDetection:
    enabled: bool
    max_duplicate_percentage: float

@dataclass
class ValidationSettings:
    enable_schema_validation: bool
    enable_data_validation: bool
    fail_on_validation_error: bool
    missing_value_thresholds: MissingValueThresholds
    outlier_detection: OutlierDetection
    duplicate_detection: DuplicateDetection

@dataclass
class SchemaValidationRules:
    strict_mode: bool
    allow_extra_columns: bool
    allow_missing_columns: bool
    validate_column_order: bool

@dataclass
class CompletenessRules:
    min_record_count: int
    max_null_percentage: float

@dataclass
class ConsistencyRules:
    check_temporal_consistency: bool
    min_trip_duration_seconds: int
    max_trip_duration_hours: int
    check_derived_features: bool
    cyclical_tolerance: float
    check_ratio_consistency: bool
    ratio_tolerance: float

@dataclass
class BusinessRules:
    check_fare_calculation: bool
    fare_tolerance: float
    check_tip_logic: bool
    max_tip_multiplier: float
    check_speed_reasonableness: bool
    min_speed_mph: float
    max_speed_mph: float
    check_route_id_consistency: bool

@dataclass
class DataQualityRules:
    completeness: CompletenessRules
    consistency: ConsistencyRules
    business_rules: BusinessRules

@dataclass
class ReportingConfig:
    generate_reports: bool
    report_format: str
    report_dir: Path
    log_to_mlflow: bool

@dataclass
class DataValidationConfig:
    validation: ValidationSettings
    schema_validation: SchemaValidationRules
    data_quality: DataQualityRules
    reporting: ReportingConfig