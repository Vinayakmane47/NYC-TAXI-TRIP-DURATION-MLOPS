"""
Centralized configuration loader with caching.

This module provides a singleton config loader that caches loaded
configurations to avoid repeated YAML parsing.
"""

import logging
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from functools import lru_cache

from src.config import (
    DataIngestionConfig,
    DataPreprocessingConfig,
    FeatureEngineeringConfig,
    DataValidationConfig,
    S3Config,
    DataCleaningConfig,
    SparkConfig,
    ProcessingConfig,
    OutputConfig,
    FeatureEngineeringSettings,
    ValidationSettings,
    SchemaValidationRules,
    DataQualityRules,
    ReportingConfig,
    MissingValueThresholds,
    OutlierDetection,
    DuplicateDetection,
    CompletenessRules,
    ConsistencyRules,
    BusinessRules,
    ParallelProcessingConfig,
    PerformanceConfig,
    IcebergConfig,
    CatalogConfig,
    TableConfig,
    TablesConfig,
    ConversionConfig,
    RegistryConfig,
)
from src.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class ConfigLoader:
    """Singleton configuration loader with caching."""

    _instance: Optional['ConfigLoader'] = None
    _config_cache: Dict[str, Any] = {}

    def __new__(cls):
        """Ensure singleton instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize ConfigLoader."""
        self.config_dir = Path(__file__).parent
        logger.debug(f"ConfigLoader initialized with config_dir: {self.config_dir}")

    @lru_cache(maxsize=None)
    def _load_yaml(self, config_file: str) -> Dict[str, Any]:
        """
        Load YAML configuration file with caching.

        Args:
            config_file: Configuration filename (e.g., 'ingestion.yaml')

        Returns:
            Dictionary with configuration data

        Raises:
            ConfigurationError: If file doesn't exist or is invalid
        """
        config_path = self.config_dir / config_file

        if not config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")

        try:
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)

            if config_data is None:
                raise ConfigurationError(f"Empty configuration file: {config_path}")

            logger.debug(f"Loaded configuration from {config_file}")
            return config_data

        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in {config_file}: {str(e)}") from e
        except Exception as e:
            raise ConfigurationError(f"Failed to load {config_file}: {str(e)}") from e

    def _parse_s3_config(self, config_data: Dict[str, Any]) -> Optional[S3Config]:
        """Parse S3 configuration from config data."""
        if 's3' not in config_data:
            return None

        s3_data = config_data['s3']
        return S3Config(
            endpoint_url=s3_data['endpoint_url'],
            access_key_id=s3_data['access_key_id'],
            secret_access_key=s3_data['secret_access_key'],
            region=s3_data.get('region', 'us-east-1'),
            use_ssl=s3_data.get('use_ssl', False),
            path_style_access=s3_data.get('path_style_access', True)
        )

    def _parse_spark_config(self, config_data: Dict[str, Any]) -> SparkConfig:
        """Parse Spark configuration from config data."""
        spark_data = config_data['spark']
        return SparkConfig(
            app_name=spark_data['app_name'],
            master=spark_data['master'],
            config=spark_data.get('config', {})
        )

    def _parse_processing_config(self, config_data: Dict[str, Any]) -> ProcessingConfig:
        """Parse processing configuration from config data."""
        processing_data = config_data['processing']
        return ProcessingConfig(
            year=processing_data['year'],
            months=processing_data['months'],
            repartition=processing_data['repartition'],
            num_partitions=processing_data['num_partitions'],
            cache_intermediate=processing_data['cache_intermediate'],
            write_partitions=processing_data.get('write_partitions', 50)
        )

    def _parse_output_config(self, config_data: Dict[str, Any]) -> OutputConfig:
        """Parse output configuration from config data."""
        output_data = config_data['output']
        return OutputConfig(
            format=output_data['format'],
            compression=output_data['compression'],
            mode=output_data['mode']
        )

    def load_ingestion_config(self) -> DataIngestionConfig:
        """
        Load data ingestion configuration.

        Returns:
            DataIngestionConfig object

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if 'ingestion' in self._config_cache:
            return self._config_cache['ingestion']

        try:
            config_data = self._load_yaml('ingestion.yaml')

            s3_config = self._parse_s3_config(config_data)

            # Parse parallel processing config (optional)
            parallel_config = None
            if 'parallel_processing' in config_data:
                pp_data = config_data['parallel_processing']
                parallel_config = ParallelProcessingConfig(
                    enabled=pp_data.get('enabled', True),
                    max_workers=pp_data.get('max_workers', 4),
                    batch_size=pp_data.get('batch_size', 4)
                )

            # Parse performance config (optional)
            performance_config = None
            if 'performance' in config_data:
                perf_data = config_data['performance']
                performance_config = PerformanceConfig(
                    remove_sleep_delays=perf_data.get('remove_sleep_delays', True),
                    async_s3_upload=perf_data.get('async_s3_upload', True),
                    connection_pooling=perf_data.get('connection_pooling', True)
                )

            config = DataIngestionConfig(
                root_dir=Path(config_data['root_dir']),
                api_base_url=config_data['api_base_url'],
                file_name_pattern=config_data['file_name_pattern'],
                year=config_data['year'],
                raw_bronze_dir=Path(config_data['raw_bronze_dir']),
                download_dir=Path(config_data['download_dir']),
                chunk_size=config_data['chunk_size'],
                max_retries=config_data['max_retries'],
                retry_delay=config_data['retry_delay'],
                validate_download=config_data['validate_download'],
                min_file_size_mb=config_data['min_file_size_mb'],
                s3=s3_config,
                bronze_bucket=config_data.get('bronze_bucket'),
                use_s3=config_data.get('use_s3', False),
                parallel_processing=parallel_config,
                performance=performance_config
            )

            self._config_cache['ingestion'] = config
            logger.info("Data ingestion configuration loaded successfully")
            return config

        except KeyError as e:
            raise ConfigurationError(f"Missing required key in ingestion config: {e}") from e
        except Exception as e:
            raise ConfigurationError(f"Failed to load ingestion config: {e}") from e

    def load_preprocessing_config(self) -> DataPreprocessingConfig:
        """
        Load data preprocessing configuration.

        Returns:
            DataPreprocessingConfig object

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if 'preprocessing' in self._config_cache:
            return self._config_cache['preprocessing']

        try:
            config_data = self._load_yaml('preprocessing.yaml')

            # Parse nested configs
            s3_config = self._parse_s3_config(config_data)
            spark_config = self._parse_spark_config(config_data)
            processing_config = self._parse_processing_config(config_data)
            output_config = self._parse_output_config(config_data)

            # Parse data cleaning config
            cleaning_data = config_data['data_cleaning']
            data_cleaning_config = DataCleaningConfig(
                min_trip_duration_min=cleaning_data['min_trip_duration_min'],
                max_trip_duration_min=cleaning_data['max_trip_duration_min'],
                min_trip_distance=cleaning_data['min_trip_distance'],
                max_trip_distance=cleaning_data['max_trip_distance'],
                min_fare_amount=cleaning_data['min_fare_amount'],
                max_fare_amount=cleaning_data['max_fare_amount'],
                min_total_amount=cleaning_data['min_total_amount'],
                max_tip_multiplier=cleaning_data['max_tip_multiplier']
            )

            config = DataPreprocessingConfig(
                root_dir=Path(config_data['root_dir']),
                raw_bronze_dir=Path(config_data['raw_bronze_dir']),
                silver_dir=Path(config_data['silver_dir']),
                artifact_dir=Path(config_data['artifact_dir']),
                data_cleaning=data_cleaning_config,
                spark=spark_config,
                processing=processing_config,
                output=output_config,
                s3=s3_config,
                bronze_bucket=config_data.get('bronze_bucket'),
                silver_bucket=config_data.get('silver_bucket'),
                use_s3=config_data.get('use_s3', False)
            )

            self._config_cache['preprocessing'] = config
            logger.info("Data preprocessing configuration loaded successfully")
            return config

        except KeyError as e:
            raise ConfigurationError(f"Missing required key in preprocessing config: {e}") from e
        except Exception as e:
            raise ConfigurationError(f"Failed to load preprocessing config: {e}") from e

    def load_feature_engineering_config(self) -> FeatureEngineeringConfig:
        """
        Load feature engineering configuration.

        Returns:
            FeatureEngineeringConfig object

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if 'feature_engineering' in self._config_cache:
            return self._config_cache['feature_engineering']

        try:
            config_data = self._load_yaml('feature_engineering.yaml')

            # Parse nested configs
            s3_config = self._parse_s3_config(config_data)
            spark_config = self._parse_spark_config(config_data)
            processing_config = self._parse_processing_config(config_data)
            output_config = self._parse_output_config(config_data)

            # Parse feature engineering settings
            fe_data = config_data['feature_engineering']
            fe_settings = FeatureEngineeringSettings(
                include_time_features=fe_data['include_time_features'],
                include_cyclical_encoding=fe_data['include_cyclical_encoding'],
                include_distance_features=fe_data['include_distance_features'],
                include_ratio_features=fe_data['include_ratio_features'],
                include_location_features=fe_data['include_location_features'],
                compute_frequency_features=fe_data['compute_frequency_features'],
                include_flag_features=fe_data['include_flag_features']
            )

            config = FeatureEngineeringConfig(
                root_dir=Path(config_data['root_dir']),
                silver_dir=Path(config_data['silver_dir']),
                gold_dir=Path(config_data['gold_dir']),
                artifact_dir=Path(config_data['artifact_dir']),
                feature_engineering=fe_settings,
                spark=spark_config,
                processing=processing_config,
                output=output_config,
                s3=s3_config,
                silver_bucket=config_data.get('silver_bucket'),
                gold_bucket=config_data.get('gold_bucket'),
                use_s3=config_data.get('use_s3', False)
            )

            self._config_cache['feature_engineering'] = config
            logger.info("Feature engineering configuration loaded successfully")
            return config

        except KeyError as e:
            raise ConfigurationError(f"Missing required key in feature engineering config: {e}") from e
        except Exception as e:
            raise ConfigurationError(f"Failed to load feature engineering config: {e}") from e

    def load_validation_config(self) -> DataValidationConfig:
        """
        Load data validation configuration.

        Returns:
            DataValidationConfig object

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if 'validation' in self._config_cache:
            return self._config_cache['validation']

        try:
            config_data = self._load_yaml('validation.yaml')

            # Parse validation settings
            val_data = config_data['validation']
            missing_val_data = val_data['missing_value_thresholds']
            outlier_data = val_data['outlier_detection']
            duplicate_data = val_data['duplicate_detection']

            validation_settings = ValidationSettings(
                enable_schema_validation=val_data['enable_schema_validation'],
                enable_data_validation=val_data['enable_data_validation'],
                fail_on_validation_error=val_data['fail_on_validation_error'],
                missing_value_thresholds=MissingValueThresholds(
                    critical_columns=missing_val_data['critical_columns'],
                    important_columns=missing_val_data['important_columns'],
                    optional_columns=missing_val_data['optional_columns']
                ),
                outlier_detection=OutlierDetection(
                    enabled=outlier_data['enabled'],
                    method=outlier_data['method'],
                    zscore_threshold=outlier_data['zscore_threshold'],
                    iqr_multiplier=outlier_data['iqr_multiplier']
                ),
                duplicate_detection=DuplicateDetection(
                    enabled=duplicate_data['enabled'],
                    max_duplicate_percentage=duplicate_data['max_duplicate_percentage']
                )
            )

            # Parse schema validation rules
            schema_data = config_data['schema_validation']
            schema_rules = SchemaValidationRules(
                strict_mode=schema_data['strict_mode'],
                allow_extra_columns=schema_data['allow_extra_columns'],
                allow_missing_columns=schema_data['allow_missing_columns'],
                validate_column_order=schema_data['validate_column_order']
            )

            # Parse data quality rules
            dq_data = config_data['data_quality']
            completeness_data = dq_data['completeness']
            consistency_data = dq_data['consistency']
            business_data = dq_data['business_rules']

            data_quality_rules = DataQualityRules(
                completeness=CompletenessRules(
                    min_record_count=completeness_data['min_record_count'],
                    max_null_percentage=completeness_data['max_null_percentage']
                ),
                consistency=ConsistencyRules(
                    check_temporal_consistency=consistency_data['check_temporal_consistency'],
                    min_trip_duration_seconds=consistency_data['min_trip_duration_seconds'],
                    max_trip_duration_hours=consistency_data['max_trip_duration_hours'],
                    check_derived_features=consistency_data['check_derived_features'],
                    cyclical_tolerance=consistency_data['cyclical_tolerance'],
                    check_ratio_consistency=consistency_data['check_ratio_consistency'],
                    ratio_tolerance=consistency_data['ratio_tolerance']
                ),
                business_rules=BusinessRules(
                    check_fare_calculation=business_data['check_fare_calculation'],
                    fare_tolerance=business_data['fare_tolerance'],
                    check_tip_logic=business_data['check_tip_logic'],
                    max_tip_multiplier=business_data['max_tip_multiplier'],
                    check_speed_reasonableness=business_data['check_speed_reasonableness'],
                    min_speed_mph=business_data['min_speed_mph'],
                    max_speed_mph=business_data['max_speed_mph'],
                    check_route_id_consistency=business_data['check_route_id_consistency']
                )
            )

            # Parse reporting config
            reporting_data = config_data['reporting']
            reporting_config = ReportingConfig(
                generate_reports=reporting_data['generate_reports'],
                report_format=reporting_data['report_format'],
                report_dir=Path(reporting_data['report_dir']),
                log_to_mlflow=reporting_data['log_to_mlflow']
            )

            config = DataValidationConfig(
                validation=validation_settings,
                schema_validation=schema_rules,
                data_quality=data_quality_rules,
                reporting=reporting_config
            )

            self._config_cache['validation'] = config
            logger.info("Data validation configuration loaded successfully")
            return config

        except KeyError as e:
            raise ConfigurationError(f"Missing required key in validation config: {e}") from e
        except Exception as e:
            raise ConfigurationError(f"Failed to load validation config: {e}") from e

    def load_iceberg_config(self) -> IcebergConfig:
        """
        Load Iceberg table management configuration.

        Returns:
            IcebergConfig object

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if 'iceberg' in self._config_cache:
            return self._config_cache['iceberg']

        try:
            config_data = self._load_yaml('iceberg.yaml')

            # Parse nested configs
            s3_config = self._parse_s3_config(config_data)
            spark_config = self._parse_spark_config(config_data)

            # Parse catalog config
            catalog_data = config_data['catalog']
            catalog_config = CatalogConfig(
                name=catalog_data['name'],
                type=catalog_data['type'],
                warehouse=catalog_data['warehouse']
            )

            # Parse tables config
            tables_data = config_data['tables']
            tables_config = TablesConfig(
                bronze=TableConfig(
                    table_name=tables_data['bronze']['table_name'],
                    schema_name=tables_data['bronze']['schema_name'],
                    location=tables_data['bronze']['location'],
                    partition_columns=tables_data['bronze']['partition_columns']
                ),
                silver=TableConfig(
                    table_name=tables_data['silver']['table_name'],
                    schema_name=tables_data['silver']['schema_name'],
                    location=tables_data['silver']['location'],
                    partition_columns=tables_data['silver']['partition_columns']
                ),
                gold=TableConfig(
                    table_name=tables_data['gold']['table_name'],
                    schema_name=tables_data['gold']['schema_name'],
                    location=tables_data['gold']['location'],
                    partition_columns=tables_data['gold']['partition_columns']
                )
            )

            # Parse conversion config
            conversion_data = config_data['conversion']
            conversion_config = ConversionConfig(
                source_paths=conversion_data['source_paths'],
                mode=conversion_data['mode'],
                create_if_not_exists=conversion_data['create_if_not_exists'],
                register_tables=conversion_data['register_tables']
            )

            # Parse registry config
            registry_data = config_data['registry']
            registry_config = RegistryConfig(
                file_path=Path(registry_data['file_path']),
                persist=registry_data['persist']
            )

            config = IcebergConfig(
                root_dir=Path(config_data['root_dir']),
                catalog=catalog_config,
                tables=tables_config,
                conversion=conversion_config,
                registry=registry_config,
                spark=spark_config,
                s3=s3_config,
                use_s3=config_data.get('use_s3', False)
            )

            self._config_cache['iceberg'] = config
            logger.info("Iceberg configuration loaded successfully")
            return config

        except KeyError as e:
            raise ConfigurationError(f"Missing required key in iceberg config: {e}") from e
        except Exception as e:
            raise ConfigurationError(f"Failed to load iceberg config: {e}") from e

    def clear_cache(self) -> None:
        """Clear the configuration cache."""
        self._config_cache.clear()
        self._load_yaml.cache_clear()
        logger.info("Configuration cache cleared")


# Singleton instance
_config_loader = ConfigLoader()


# Convenience functions
def load_ingestion_config() -> DataIngestionConfig:
    """Load data ingestion configuration."""
    return _config_loader.load_ingestion_config()


def load_preprocessing_config() -> DataPreprocessingConfig:
    """Load data preprocessing configuration."""
    return _config_loader.load_preprocessing_config()


def load_feature_engineering_config() -> FeatureEngineeringConfig:
    """Load feature engineering configuration."""
    return _config_loader.load_feature_engineering_config()


def load_validation_config() -> DataValidationConfig:
    """Load data validation configuration."""
    return _config_loader.load_validation_config()


def load_iceberg_config() -> IcebergConfig:
    """Load Iceberg table management configuration."""
    return _config_loader.load_iceberg_config()


def clear_config_cache() -> None:
    """Clear the configuration cache."""
    _config_loader.clear_cache()
