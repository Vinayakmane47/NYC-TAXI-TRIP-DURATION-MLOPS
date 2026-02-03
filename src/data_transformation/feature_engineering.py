"""
Feature Engineering Module for NYC Yellow Taxi Trip Data using PySpark

This module performs feature engineering on silver layer data
and saves enriched data to gold layer.

Refactored to use BaseDataTransformationStage for code reuse.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

from pyspark.sql import functions as F

from src.config import FeatureEngineeringConfig, FeatureEngineeringSettings
from src.config.loader import load_feature_engineering_config, load_validation_config
from src.data_transformation.base_stage import BaseDataTransformationStage
from src.data_validation.data_validation import DataValidation
from src.exceptions import DataTransformationError, ConfigurationError
from src.utils.memory_utils import MemoryManager, configure_memory_optimized_spark

logger = logging.getLogger(__name__)


class FeatureEngineeringPipeline:
    """OOP pipeline for NYC Taxi Data feature engineering using PySpark."""

    def __init__(self, settings: FeatureEngineeringSettings):
        self.settings = settings

    def add_time_features(self, df: DataFrame) -> DataFrame:
        """Add core and cyclical time features to the DataFrame."""
        if not self.settings.include_time_features:
            return df

        logger.info("Adding time-based features...")
        df = (
            df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
              .withColumn("pickup_day_of_week", (F.dayofweek("tpep_pickup_datetime") + 5) % 7)
              .withColumn("pickup_day_of_month", F.dayofmonth("tpep_pickup_datetime"))
              .withColumn("pickup_week_of_year", F.weekofyear("tpep_pickup_datetime"))
              .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
              .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
        )

        # Binary flags
        df = df.withColumn("is_weekend", F.col("pickup_day_of_week").isin([5, 6]))
        df = df.withColumn(
            "is_peak_hour", F.col("pickup_hour").isin(list(range(7, 11)) + list(range(16, 20)))
        )
        df = df.withColumn(
            "is_night", F.col("pickup_hour").isin([22, 23, 0, 1, 2, 3, 4, 5])
        )
        df = df.withColumn("is_rush_hour", F.col("is_peak_hour"))

        # Cyclical encoding
        if self.settings.include_cyclical_encoding:
            df = df.withColumn(
                "sin_hour",
                F.sin(2 * 3.141592653589793 * F.col("pickup_hour") / 24)
            ).withColumn(
                "cos_hour",
                F.cos(2 * 3.141592653589793 * F.col("pickup_hour") / 24)
            ).withColumn(
                "sin_day_of_week",
                F.sin(2 * 3.141592653589793 * F.col("pickup_day_of_week") / 7)
            ).withColumn(
                "cos_day_of_week",
                F.cos(2 * 3.141592653589793 * F.col("pickup_day_of_week") / 7)
            )

        return df

    def add_distance_features(self, df: DataFrame) -> DataFrame:
        """Add log/sqrt/flagged features based on trip_distance."""
        if not self.settings.include_distance_features:
            return df

        logger.info("Adding distance-based features...")
        df = df.withColumn("is_short_trip", F.col("trip_distance") < 1)
        df = df.withColumn("is_long_trip", F.col("trip_distance") > 10)
        df = df.withColumn("log_trip_distance", F.log1p(F.col("trip_distance")))
        df = df.withColumn("sqrt_trip_distance", F.sqrt(F.col("trip_distance")))
        return df

    def add_ratio_features(self, df: DataFrame) -> DataFrame:
        """Add fare per mile, surcharge_ratio, and toll_ratio."""
        if not self.settings.include_ratio_features:
            return df

        logger.info("Adding ratio features...")
        # Avoid division by zero
        df = df.withColumn(
            "fare_per_mile",
            F.when(F.col("trip_distance") > 0,
                   F.col("fare_amount") / F.col("trip_distance")).otherwise(F.lit(None))
        )
        surcharge_total = (
            F.coalesce(F.col("extra"), F.lit(0)) +
            F.coalesce(F.col("congestion_surcharge"), F.lit(0)) +
            F.coalesce(F.col("Airport_fee"), F.lit(0)) +
            F.coalesce(F.col("cbd_congestion_fee"), F.lit(0))
        )
        df = df.withColumn(
            "surcharge_ratio",
            F.when(F.col("total_amount") != 0,
                   surcharge_total / F.col("total_amount")
            ).otherwise(F.lit(None))
        )
        df = df.withColumn(
            "toll_ratio",
            F.when(F.col("total_amount") != 0,
                   F.col("tolls_amount") / F.col("total_amount")
            ).otherwise(F.lit(None))
        )
        return df

    def add_flags_and_consistency(self, df: DataFrame) -> DataFrame:
        """Add has_tolls, has_airport_fee, has_congestion_fee, non_fare_amount."""
        if not self.settings.include_flag_features:
            return df

        logger.info("Adding flag features...")
        df = df.withColumn(
            "has_tolls",
            (F.coalesce(F.col("tolls_amount"), F.lit(0)) > 0).cast("int")
        )
        df = df.withColumn(
            "has_airport_fee",
            (F.coalesce(F.col("Airport_fee"), F.lit(0)) > 0).cast("int")
        )
        df = df.withColumn(
            "has_congestion_fee",
            (F.coalesce(F.col("congestion_surcharge"), F.lit(0)) > 0).cast("int")
        )
        df = df.withColumn(
            "non_fare_amount",
            F.col("total_amount") - F.col("fare_amount")
        )
        return df

    def add_location_features(self, df: DataFrame) -> DataFrame:
        """Add route_id, is_same_zone, trip frequency features, and categorical IDs."""
        if not self.settings.include_location_features:
            return df

        logger.info("Adding location-based features...")
        # Convert IDs to int
        df = df.withColumn("PULocationID", F.col("PULocationID").cast("int"))
        df = df.withColumn("DOLocationID", F.col("DOLocationID").cast("int"))

        # route_id feature
        df = df.withColumn(
            "route_id",
            F.concat_ws('_', F.col("PULocationID").cast("string"), F.col("DOLocationID").cast("string"))
        )
        df = df.withColumn(
            "is_same_zone",
            (F.col("PULocationID") == F.col("DOLocationID"))
        )

        # Frequency features (note: in production, learn on train only to avoid leakage)
        # MEMORY OPTIMIZATION: Use broadcast joins for small aggregated DataFrames
        if self.settings.compute_frequency_features:
            logger.info("Computing frequency features with memory-efficient strategy...")

            # Compute all aggregations in a single pass to minimize memory usage
            # Use broadcast hint for small aggregated DataFrames
            from pyspark.sql.functions import broadcast

            # Aggregate pickup counts (small result, safe to broadcast)
            pickup_counts = (
                df.groupBy("PULocationID")
                .count()
                .withColumnRenamed("count", "pickup_zone_trip_count")
            )
            logger.info(f"Computed pickup zone counts")

            # Join with broadcast to avoid shuffle on large DataFrame
            df = df.join(broadcast(pickup_counts), on="PULocationID", how="left")
            logger.info("Joined pickup zone counts (broadcast)")

            # Clear pickup_counts from memory
            pickup_counts.unpersist()
            del pickup_counts

            # Aggregate dropoff counts
            dropoff_counts = (
                df.groupBy("DOLocationID")
                .count()
                .withColumnRenamed("count", "dropoff_zone_trip_count")
            )
            logger.info(f"Computed dropoff zone counts")

            # Join with broadcast
            df = df.join(broadcast(dropoff_counts), on="DOLocationID", how="left")
            logger.info("Joined dropoff zone counts (broadcast)")

            # Clear dropoff_counts from memory
            dropoff_counts.unpersist()
            del dropoff_counts

            # Aggregate route counts
            route_counts = (
                df.groupBy("route_id")
                .count()
                .withColumnRenamed("count", "route_trip_count")
            )
            logger.info(f"Computed route counts")

            # Join with broadcast
            df = df.join(broadcast(route_counts), on="route_id", how="left")
            logger.info("Joined route counts (broadcast)")

            # Clear route_counts from memory
            route_counts.unpersist()
            del route_counts

        # Convert to categorical (Spark uses string category for ML)
        df = df.withColumn("PULocationID", F.col("PULocationID").cast("string"))
        df = df.withColumn("DOLocationID", F.col("DOLocationID").cast("string"))
        return df

    def final_select(self, df: DataFrame) -> DataFrame:
        """Drop columns not required for modeling."""
        return df.drop("tpep_dropoff_datetime", "total_amount")

    def run(self, df: DataFrame) -> DataFrame:
        """
        Complete feature engineering pipeline.

        Args:
            df: Input DataFrame from silver layer

        Returns:
            DataFrame with engineered features
        """
        logger.info("Starting feature engineering pipeline...")
        df = self.add_time_features(df)
        df = self.add_distance_features(df)
        df = self.add_ratio_features(df)
        df = self.add_flags_and_consistency(df)
        df = self.add_location_features(df)
        df = self.final_select(df)
        logger.info("Feature engineering pipeline completed")
        return df


class FeatureEngineering(BaseDataTransformationStage):
    """
    Feature engineering stage that enriches silver data and saves to gold layer.

    Inherits from BaseDataTransformationStage to eliminate code duplication.
    """

    def __init__(
        self,
        config: Optional[FeatureEngineeringConfig] = None,
        use_existing_spark: Optional[SparkSession] = None
    ):
        """
        Initialize FeatureEngineering.

        Args:
            config: FeatureEngineeringConfig object. If None, loads from YAML.
            use_existing_spark: Optional existing SparkSession to reuse.
        """
        # Load config if not provided
        if config is None:
            try:
                config = load_feature_engineering_config()
            except Exception as e:
                raise ConfigurationError(
                    f"Failed to load feature engineering config: {e}"
                ) from e

        # Initialize base class
        super().__init__(config, use_existing_spark)

        # Initialize memory manager for OOM prevention
        self.memory_manager = MemoryManager(self.spark)

        # Apply memory-optimized Spark configurations
        configure_memory_optimized_spark(self.spark)

        # Initialize feature engineering pipeline
        self.feature_pipeline = FeatureEngineeringPipeline(
            self.config.feature_engineering
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
        """Ensure silver and gold buckets exist."""
        if self.config.silver_bucket and self.config.gold_bucket:
            self.s3_client.ensure_buckets_exist(
                self.config.silver_bucket,
                self.config.gold_bucket
            )

    def _setup_directories(self) -> None:
        """Create necessary directories for local storage."""
        self.config.root_dir.mkdir(parents=True, exist_ok=True)
        self.config.gold_dir.mkdir(parents=True, exist_ok=True)
        self.config.artifact_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Directories created/verified: {self.config.gold_dir}")

    def _get_input_bucket(self) -> Optional[str]:
        """Get silver bucket name."""
        return self.config.silver_bucket

    def _get_output_bucket(self) -> Optional[str]:
        """Get gold bucket name."""
        return self.config.gold_bucket

    def _get_input_directory(self) -> Path:
        """Get silver directory path."""
        return self.config.silver_dir

    def _get_output_directory(self) -> Path:
        """Get gold directory path."""
        return self.config.gold_dir

    def _get_s3_prefix_pattern(self, year: int, month: int) -> str:
        """Get S3 prefix pattern for silver layer."""
        return f"silver/{year}/{month:02d}/"

    def _transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply feature engineering transformations with memory management.

        Args:
            df: Input DataFrame from silver layer

        Returns:
            DataFrame with engineered features for gold layer
        """
        logger.info("Starting feature engineering transformations...")

        # Get initial column count
        initial_cols = len(df.columns)

        # Clear cache before starting transformations
        self.memory_manager.clear_cache()

        # Optimize partitions before heavy transformations
        df = self.memory_manager.repartition_if_needed(df, target_partitions=50)

        # Apply feature engineering pipeline
        enriched_df = self.feature_pipeline.run(df)

        # Clear cache of intermediate DataFrames
        self.memory_manager.clear_cache(df)

        # Optimize partitions after transformations (may have grown)
        enriched_df = self.memory_manager.repartition_if_needed(enriched_df, target_partitions=50)

        # Get final column count
        final_cols = len(enriched_df.columns)
        added_cols = final_cols - initial_cols

        logger.info(
            f"Feature engineering complete: {initial_cols} -> {final_cols} columns "
            f"({added_cols} features added)"
        )

        return enriched_df

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
            Processed DataFrame with engineered features

        Raises:
            DataTransformationError: If processing fails
        """
        try:
            # Use config defaults if not specified
            year = year or self.config.processing.year
            months = months or self.config.processing.months

            logger.info(f"Processing data for year {year}, months {months}")

            # Get input files from silver layer
            file_paths = self._get_data_files(year, months, layer="silver")

            if not file_paths:
                raise DataTransformationError(
                    f"No data files found for year {year}, months {months}"
                )

            # Read data
            silver_df = self._read_data(file_paths)

            # Transform data
            gold_df = self._transform_data(silver_df)

            # Validate if requested and validator is available
            if validate and self.validator:
                logger.info("Running data validation...")
                validation_report = self.validator.validate(gold_df)
                logger.info(f"Validation report: {validation_report}")

            return gold_df

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
        Process data and save to gold layer.

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
            gold_df = self.process(year, months, validate)

            # Determine output path - save each month separately to directory (not filename)
            # If processing single month, save to month-specific directory
            if months and len(months) == 1:
                month = months[0]
                output_path = self.config.gold_dir / f"{year}/{month:02d}"
            else:
                # Multiple months - save to year directory
                output_path = self.config.gold_dir / f"{year}"

            # Save data (layer parameter is kept for backward compatibility but not used in S3 path)
            save_path = self._save_data(gold_df, output_path, layer="gold")

            logger.info(f"Processing and saving completed successfully: {save_path}")
            return save_path

        except Exception as e:
            logger.error(f"Process and save failed: {str(e)}")
            raise DataTransformationError(
                f"Process and save failed: {str(e)}"
            ) from e


# Convenience function for backward compatibility
def load_feature_engineering_module(
    config: Optional[FeatureEngineeringConfig] = None,
    use_existing_spark: Optional[SparkSession] = None
) -> FeatureEngineering:
    """
    Load and initialize FeatureEngineering module.

    Args:
        config: Optional config. If None, loads from YAML
        use_existing_spark: Optional existing SparkSession

    Returns:
        Initialized FeatureEngineering instance
    """
    return FeatureEngineering(config, use_existing_spark)
