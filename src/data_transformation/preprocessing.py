"""
Data Preprocessing Module for NYC Yellow Taxi Trip Data using PySpark

This module performs data cleaning and preprocessing on raw/bronze data
and saves processed data to silver layer.

Refactored to use BaseDataTransformationStage for code reuse.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, List, TYPE_CHECKING
from functools import reduce

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

from pyspark.sql import functions as F
from pydantic import BaseModel

from src.config import DataPreprocessingConfig
from src.config.loader import load_preprocessing_config, load_validation_config
from src.data_transformation.base_stage import BaseDataTransformationStage
from src.data_validation.data_validation import DataValidation
from src.exceptions import DataTransformationError, ConfigurationError
from src.utils.memory_utils import MemoryManager, configure_memory_optimized_spark

logger = logging.getLogger(__name__)


class DataCleaningConfigModel(BaseModel):
    """Pydantic config for data cleaning pipeline."""
    min_trip_duration_min: float = 0.0
    max_trip_duration_min: float = 180.0
    min_trip_distance: float = 0.0
    max_trip_distance: float = 60.0
    min_fare_amount: float = 0.0
    max_fare_amount: float = 500.0
    min_total_amount: float = 0.0
    max_tip_multiplier: float = 2.0


class DataCleaningPipeline:
    """OOP pipeline for NYC Taxi Data cleaning using PySpark."""

    def __init__(self, config: DataCleaningConfigModel):
        self.config = config

    def select_columns(self, df: DataFrame) -> DataFrame:
        """Select only columns needed for modeling."""
        columns = [
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "RatecodeID",
            "store_and_fwd_flag", "PULocationID", "DOLocationID",
            "payment_type", "fare_amount", "extra", "mta_tax",
            "tip_amount", "tolls_amount", "improvement_surcharge",
            "total_amount", "congestion_surcharge", "Airport_fee",
            "cbd_congestion_fee", "trip_duration_min"
        ]
        # Only select columns that exist
        existing_cols = [c for c in columns if c in df.columns]
        return df.select(*existing_cols)

    def compute_trip_duration(self, df: DataFrame) -> DataFrame:
        """Compute trip duration in minutes and add as new column."""
        return df.withColumn(
            "trip_duration_min",
            (F.unix_timestamp("tpep_dropoff_datetime") -
             F.unix_timestamp("tpep_pickup_datetime")) / 60.0
        )

    def remove_invalid_durations(self, df: DataFrame) -> DataFrame:
        """Remove rows with invalid or extreme trip durations."""
        return (
            df.filter(F.col("trip_duration_min").isNotNull())
              .filter(
                  (F.col("trip_duration_min") > self.config.min_trip_duration_min) &
                  (F.col("trip_duration_min") < self.config.max_trip_duration_min)
              )
        )

    def remove_invalid_distances(self, df: DataFrame) -> DataFrame:
        """Remove rows with invalid or extreme trip distances."""
        return (
            df.filter(F.col("trip_distance").isNotNull())
              .filter(
                  (F.col("trip_distance") > self.config.min_trip_distance) &
                  (F.col("trip_distance") < self.config.max_trip_distance)
              )
        )

    def remove_invalid_monetary(self, df: DataFrame) -> DataFrame:
        """Remove rows with negative or extreme fare and total amounts."""
        return (
            df.filter(F.col("fare_amount").isNotNull())
              .filter(
                  (F.col("fare_amount") > self.config.min_fare_amount) &
                  (F.col("fare_amount") < self.config.max_fare_amount)
              )
              .filter(F.col("total_amount").isNotNull())
              .filter(F.col("total_amount") >= self.config.min_total_amount)
        )

    def remove_missing_critical(self, df: DataFrame) -> DataFrame:
        """Remove rows missing critical columns for ML/EDA."""
        critical_cols = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID"
        ]
        conditions = [F.col(c).isNotNull() for c in critical_cols]
        combined_cond = reduce(lambda a, b: a & b, conditions)
        return df.filter(combined_cond)

    def impute_missing_values(self, df: DataFrame) -> DataFrame:
        """Impute common missing values based on domain knowledge."""
        return df.fillna({
            'passenger_count': 1,
            'RatecodeID': 1,
            'congestion_surcharge': 0,
            'Airport_fee': 0,
            'store_and_fwd_flag': 'N'
        })

    def clip_ratecode(self, df: DataFrame) -> DataFrame:
        """Clip RatecodeID to valid range [1,6]."""
        return df.withColumn(
            "RatecodeID",
            F.when(F.col("RatecodeID") < 1, 1)
             .when(F.col("RatecodeID") > 6, 6)
             .otherwise(F.col("RatecodeID"))
        )

    def clip_tip_amount(self, df: DataFrame) -> DataFrame:
        """Clip tip_amount to be within [0, fare_amount * max_tip_multiplier]."""
        return df.withColumn(
            "tip_amount",
            F.when(F.col("tip_amount") < 0, 0)
             .when(
                 F.col("tip_amount") > F.col("fare_amount") * self.config.max_tip_multiplier,
                 F.col("fare_amount") * self.config.max_tip_multiplier
             ).otherwise(F.col("tip_amount"))
        )

    def run(self, df: DataFrame) -> DataFrame:
        """
        Complete cleaning pipeline.

        Args:
            df: Input DataFrame

        Returns:
            Cleaned DataFrame
        """
        df = self.compute_trip_duration(df)
        df = self.select_columns(df)
        df = self.remove_invalid_durations(df)
        df = self.remove_invalid_distances(df)
        df = self.remove_invalid_monetary(df)
        df = self.remove_missing_critical(df)
        df = self.impute_missing_values(df)
        df = self.clip_ratecode(df)
        df = self.clip_tip_amount(df)
        return df


class DataPreprocessing(BaseDataTransformationStage):
    """
    Data preprocessing stage that cleans bronze data and saves to silver layer.

    Inherits from BaseDataTransformationStage to eliminate code duplication.
    """

    def __init__(
        self,
        config: Optional[DataPreprocessingConfig] = None,
        use_existing_spark: Optional[SparkSession] = None
    ):
        """
        Initialize DataPreprocessing.

        Args:
            config: DataPreprocessingConfig object. If None, loads from YAML.
            use_existing_spark: Optional existing SparkSession to reuse.
        """
        # Load config if not provided
        if config is None:
            try:
                config = load_preprocessing_config()
            except Exception as e:
                raise ConfigurationError(
                    f"Failed to load preprocessing config: {e}"
                ) from e

        # Initialize base class
        super().__init__(config, use_existing_spark)

        # Initialize memory manager for OOM prevention
        self.memory_manager = MemoryManager(self.spark)

        # Apply memory-optimized Spark configurations
        configure_memory_optimized_spark(self.spark)

        # Initialize cleaning pipeline
        self.cleaning_pipeline = DataCleaningPipeline(
            DataCleaningConfigModel(**self.config.data_cleaning.__dict__)
        )

        # Initialize validation (optional)
        self.validator = None
        try:
            validation_config = load_validation_config()
            self.validator = DataValidation(validation_config, spark=self.spark)
            logger.info("Data validation initialized")
        except Exception as e:
            logger.warning(
                f"Validation config not found or invalid: {e}. "
                "Continuing without validation."
            )

    def _ensure_buckets_exist(self) -> None:
        """Ensure bronze and silver buckets exist."""
        if self.config.bronze_bucket and self.config.silver_bucket:
            self.s3_client.ensure_buckets_exist(
                self.config.bronze_bucket,
                self.config.silver_bucket
            )

    def _setup_directories(self) -> None:
        """Create necessary directories for local storage."""
        self.config.root_dir.mkdir(parents=True, exist_ok=True)
        self.config.silver_dir.mkdir(parents=True, exist_ok=True)
        self.config.artifact_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Directories created/verified: {self.config.silver_dir}")

    def _get_input_bucket(self) -> Optional[str]:
        """Get bronze bucket name."""
        return self.config.bronze_bucket

    def _get_output_bucket(self) -> Optional[str]:
        """Get silver bucket name."""
        return self.config.silver_bucket

    def _get_input_directory(self) -> Path:
        """Get bronze directory path."""
        return self.config.raw_bronze_dir

    def _get_output_directory(self) -> Path:
        """Get silver directory path."""
        return self.config.silver_dir

    def _get_s3_prefix_pattern(self, year: int, month: int) -> str:
        """Get S3 prefix pattern for bronze layer."""
        return f"bronze/{year}/{month:02d}/"

    def _transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply data cleaning transformations with memory management.

        Args:
            df: Input DataFrame from bronze layer

        Returns:
            Cleaned DataFrame for silver layer
        """
        logger.info("Starting data cleaning transformations...")

        # Clear cache before starting
        self.memory_manager.clear_cache()

        # Optimize partitions before transformations
        df = self.memory_manager.repartition_if_needed(df, target_partitions=8)

        # Apply cleaning pipeline
        cleaned_df = self.cleaning_pipeline.run(df)

        # Clear intermediate DataFrame from memory
        self.memory_manager.clear_cache(df)

        # Skip row counts to prevent OOM - just log completion
        logger.info("Data cleaning completed (counts skipped to prevent OOM)")

        return cleaned_df

    def process(
        self,
        year: Optional[int] = None,
        months: Optional[List[int]] = None,
        validate: bool = True
    ) -> DataFrame:
        """
        Process data for the specified year and months.

        Args:
            year: Year to process. If None, uses config.processing.year
            months: List of months to process. If None, uses config.processing.months
            validate: Whether to run validation on the result

        Returns:
            Processed DataFrame

        Raises:
            DataTransformationError: If processing fails
        """
        try:
            # Use config defaults if not specified
            year = year or self.config.processing.year
            months = months or self.config.processing.months

            logger.info(f"Processing data for year {year}, months {months}")

            # Get input files
            file_paths = self._get_data_files(year, months, layer="bronze")

            if not file_paths:
                raise DataTransformationError(
                    f"No data files found for year {year}, months {months}"
                )

            # Read data
            raw_df = self._read_data(file_paths)

            # Transform data
            processed_df = self._transform_data(raw_df)

            # Validate if requested and validator is available
            if validate and self.validator:
                logger.info("Running data validation...")
                validation_report = self.validator.validate(processed_df)
                logger.info(f"Validation report: {validation_report}")

            return processed_df

        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            raise DataTransformationError(f"Processing failed: {str(e)}") from e

    def process_and_save(
        self,
        year: Optional[int] = None,
        months: Optional[List[int]] = None,
        validate: bool = True
    ) -> str:
        """
        Process data and save to silver layer.

        Args:
            year: Year to process. If None, uses config.processing.year
            months: List of months to process. If None, uses config.processing.months
            validate: Whether to run validation on the result

        Returns:
            Path where data was saved (S3 URI or local path)

        Raises:
            DataTransformationError: If processing or saving fails
        """
        try:
            # Use config defaults if not specified
            year = year or self.config.processing.year
            months = months or self.config.processing.months

            # Process data
            processed_df = self.process(year, months, validate)

            # Determine output path - save each month separately to directory (not filename)
            # If processing single month, save to month-specific directory
            if months and len(months) == 1:
                month = months[0]
                output_path = self.config.silver_dir / f"{year}/{month:02d}"
            else:
                # Multiple months - save to year directory
                output_path = self.config.silver_dir / f"{year}"

            # Save data (layer parameter is kept for backward compatibility but not used in S3 path)
            save_path = self._save_data(processed_df, output_path, layer="silver")

            logger.info(f"Processing and saving completed successfully: {save_path}")
            return save_path

        except Exception as e:
            logger.error(f"Process and save failed: {str(e)}")
            raise DataTransformationError(
                f"Process and save failed: {str(e)}"
            ) from e


# Convenience function for backward compatibility
def load_preprocessing_module(
    config: Optional[DataPreprocessingConfig] = None,
    use_existing_spark: Optional[SparkSession] = None
) -> DataPreprocessing:
    """
    Load and initialize DataPreprocessing module.

    Args:
        config: Optional config. If None, loads from YAML
        use_existing_spark: Optional existing SparkSession

    Returns:
        Initialized DataPreprocessing instance
    """
    return DataPreprocessing(config, use_existing_spark)
