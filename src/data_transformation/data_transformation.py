"""
Data Transformation Module for NYC Yellow Taxi Trip Data using PySpark

This module combines data preprocessing and feature engineering into a unified
transformation pipeline.

Refactored to use the new base class architecture and eliminate code duplication.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

from src.config import DataPreprocessingConfig, FeatureEngineeringConfig
from src.config.loader import (
    load_preprocessing_config,
    load_feature_engineering_config
)
from src.utils.spark_utils import SparkSessionFactory
from src.exceptions import DataTransformationError, ConfigurationError

logger = logging.getLogger(__name__)


class DataTransformation:
    """
    Unified data transformation pipeline combining preprocessing and feature engineering.

    Manages the complete ETL flow: Bronze -> Silver -> Gold
    """

    def __init__(
        self,
        preprocessing_config: Optional[DataPreprocessingConfig] = None,
        feature_engineering_config: Optional[FeatureEngineeringConfig] = None,
        use_existing_spark: Optional[SparkSession] = None
    ):
        """
        Initialize DataTransformation pipeline.

        Args:
            preprocessing_config: DataPreprocessingConfig object. If None, loads from YAML.
            feature_engineering_config: FeatureEngineeringConfig object. If None, loads from YAML.
            use_existing_spark: Optional SparkSession to reuse across stages.

        Raises:
            ConfigurationError: If configuration loading fails
        """
        try:
            # Lazy imports to avoid PySpark dependency at module level
            from src.data_transformation.preprocessing import DataPreprocessing
            from src.data_transformation.feature_engineering import FeatureEngineering

            # Load configs if not provided
            self.preprocessing_config = preprocessing_config or load_preprocessing_config()
            self.feature_engineering_config = feature_engineering_config or load_feature_engineering_config()

            # Create or use existing SparkSession
            if use_existing_spark:
                self.spark = use_existing_spark
                self._owns_spark = False
            else:
                self.spark = SparkSessionFactory.create_session(
                    self.preprocessing_config.spark,
                    force_new=False
                )
                self._owns_spark = True

            # Initialize stages with shared SparkSession
            self.preprocessing = DataPreprocessing(
                self.preprocessing_config,
                use_existing_spark=self.spark
            )
            self.feature_engineering = FeatureEngineering(
                self.feature_engineering_config,
                use_existing_spark=self.spark
            )

            logger.info("DataTransformation pipeline initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize DataTransformation: {str(e)}")
            raise ConfigurationError(
                f"DataTransformation initialization failed: {str(e)}"
            ) from e

    def transform(
        self,
        year: Optional[int] = None,
        months: Optional[List[int]] = None,
        validate: bool = True
    ) -> DataFrame:
        """
        Run complete transformation pipeline (preprocessing + feature engineering).

        Args:
            year: Year to process. If None, uses config defaults
            months: List of months to process. If None, uses config defaults
            validate: Whether to run validation checks

        Returns:
            Transformed Spark DataFrame (gold layer)

        Raises:
            DataTransformationError: If transformation fails
        """
        try:
            logger.info("=" * 80)
            logger.info("STARTING COMPLETE DATA TRANSFORMATION PIPELINE")
            logger.info("=" * 80)

            # Step 1: Preprocessing (Bronze -> Silver)
            logger.info("\n" + "=" * 80)
            logger.info("STAGE 1: Data Preprocessing (Bronze -> Silver)")
            logger.info("=" * 80)
            silver_df = self.preprocessing.process(year, months, validate)
            logger.info("Preprocessing complete.")

            # Cache silver_df if we're going to use it for feature engineering
            silver_df.cache()

            # Step 2: Feature Engineering (Silver -> Gold)
            logger.info("\n" + "=" * 80)
            logger.info("STAGE 2: Feature Engineering (Silver -> Gold)")
            logger.info("=" * 80)

            # Save silver_df temporarily for feature engineering to read
            # (In real scenarios, silver_df is already saved and FE reads from storage)
            # Here we can pass the DataFrame directly to avoid re-reading

            # For now, run feature engineering from silver storage
            gold_df = self.feature_engineering.process(year, months, validate)
            logger.info("Feature engineering complete.")

            # Unpersist cached DataFrame
            silver_df.unpersist()

            logger.info("\n" + "=" * 80)
            logger.info("DATA TRANSFORMATION PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)

            return gold_df

        except Exception as e:
            logger.error(f"Transformation pipeline failed: {str(e)}")
            raise DataTransformationError(
                f"Transformation pipeline failed: {str(e)}"
            ) from e

    def transform_and_save(
        self,
        year: Optional[int] = None,
        months: Optional[List[int]] = None,
        validate: bool = True
    ) -> Tuple[str, str]:
        """
        Run complete transformation pipeline and save results to both layers.

        Args:
            year: Year to process. If None, uses config defaults
            months: List of months to process. If None, uses config defaults
            validate: Whether to run validation checks

        Returns:
            Tuple of (silver_path, gold_path) - paths where data was saved

        Raises:
            DataTransformationError: If transformation or saving fails
        """
        try:
            logger.info("=" * 80)
            logger.info("STARTING COMPLETE DATA TRANSFORMATION PIPELINE WITH SAVE")
            logger.info("=" * 80)

            # Step 1: Preprocessing and save to silver
            logger.info("\n" + "=" * 80)
            logger.info("STAGE 1: Data Preprocessing (Bronze -> Silver)")
            logger.info("=" * 80)
            silver_path = self.preprocessing.process_and_save(year, months, validate)
            logger.info(f"Preprocessing complete. Saved to: {silver_path}")

            # Step 2: Feature Engineering and save to gold
            logger.info("\n" + "=" * 80)
            logger.info("STAGE 2: Feature Engineering (Silver -> Gold)")
            logger.info("=" * 80)
            gold_path = self.feature_engineering.process_and_save(year, months, validate)
            logger.info(f"Feature engineering complete. Saved to: {gold_path}")

            logger.info("\n" + "=" * 80)
            logger.info("DATA TRANSFORMATION PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"Silver layer: {silver_path}")
            logger.info(f"Gold layer: {gold_path}")
            logger.info("=" * 80)

            return silver_path, gold_path

        except Exception as e:
            logger.error(f"Transform and save pipeline failed: {str(e)}")
            raise DataTransformationError(
                f"Transform and save pipeline failed: {str(e)}"
            ) from e

    def stop(self) -> None:
        """
        Stop the SparkSession if this instance owns it.

        Note: Individual stages (preprocessing, feature_engineering) also have their
        own stop() methods, but they won't stop the shared SparkSession.
        """
        if self._owns_spark:
            logger.info("Stopping DataTransformation SparkSession...")
            SparkSessionFactory.stop_session()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        self.stop()
        return False


# Convenience functions for creating transformation instances
def create_preprocessing_pipeline(
    config: Optional[DataPreprocessingConfig] = None,
    use_existing_spark: Optional[SparkSession] = None
):
    """
    Create a preprocessing pipeline instance.

    Args:
        config: Optional config. If None, loads from YAML
        use_existing_spark: Optional SparkSession to reuse

    Returns:
        Initialized DataPreprocessing instance
    """
    from src.data_transformation.preprocessing import DataPreprocessing
    return DataPreprocessing(config, use_existing_spark)


def create_feature_engineering_pipeline(
    config: Optional[FeatureEngineeringConfig] = None,
    use_existing_spark: Optional[SparkSession] = None
):
    """
    Create a feature engineering pipeline instance.

    Args:
        config: Optional config. If None, loads from YAML
        use_existing_spark: Optional SparkSession to reuse

    Returns:
        Initialized FeatureEngineering instance
    """
    from src.data_transformation.feature_engineering import FeatureEngineering
    return FeatureEngineering(config, use_existing_spark)


def create_full_pipeline(
    preprocessing_config: Optional[DataPreprocessingConfig] = None,
    feature_engineering_config: Optional[FeatureEngineeringConfig] = None,
    use_existing_spark: Optional[SparkSession] = None
) -> DataTransformation:
    """
    Create a complete transformation pipeline instance.

    Args:
        preprocessing_config: Optional preprocessing config. If None, loads from YAML
        feature_engineering_config: Optional FE config. If None, loads from YAML
        use_existing_spark: Optional SparkSession to reuse

    Returns:
        Initialized DataTransformation instance
    """
    return DataTransformation(
        preprocessing_config,
        feature_engineering_config,
        use_existing_spark
    )
