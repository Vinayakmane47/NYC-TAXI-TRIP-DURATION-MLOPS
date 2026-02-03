"""
Spark utilities for creating and managing SparkSessions.

This module provides a factory for creating configured SparkSessions,
eliminating code duplication across data transformation stages.
"""

from __future__ import annotations

import logging
from typing import Optional, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

from src.config import SparkConfig
from src.exceptions import SparkSessionError

logger = logging.getLogger(__name__)


class SparkSessionFactory:
    """Factory class for creating and managing SparkSessions."""

    _instance: Optional[SparkSession] = None

    @classmethod
    def create_session(
        cls,
        spark_config: SparkConfig,
        force_new: bool = False
    ):
        """
        Create and configure a SparkSession.

        Args:
            spark_config: SparkConfig object with Spark configuration
            force_new: If True, stops existing session and creates new one

        Returns:
            Configured SparkSession

        Raises:
            SparkSessionError: If SparkSession creation fails
        """
        # Lazy import to avoid PySpark dependency at module level
        from pyspark.sql import SparkSession

        try:
            # Stop existing session if force_new or if one exists and we want to avoid conflicts
            if force_new or cls._instance is not None:
                cls.stop_session()

            # Check for any active session
            existing_spark = SparkSession.getActiveSession()
            if existing_spark and force_new:
                logger.info("Stopping existing SparkSession...")
                existing_spark.stop()
            elif existing_spark and not force_new:
                logger.info("Reusing existing SparkSession")
                cls._instance = existing_spark
                return existing_spark

            logger.info(f"Creating SparkSession: {spark_config.app_name}")

            # Build SparkSession
            builder = SparkSession.builder.appName(spark_config.app_name)

            # Set master
            if spark_config.master:
                builder = builder.master(spark_config.master)

            # Set Spark configurations
            for key, value in spark_config.config.items():
                builder = builder.config(key, value)

            # Ensure driver host is set for local mode
            if spark_config.master and "local" in spark_config.master:
                if "spark.driver.host" not in spark_config.config:
                    builder = builder.config("spark.driver.host", "localhost")
                if "spark.driver.bindAddress" not in spark_config.config:
                    builder = builder.config("spark.driver.bindAddress", "127.0.0.1")

            spark = builder.getOrCreate()
            cls._instance = spark

            logger.info(f"SparkSession created: {spark.sparkContext.applicationId}")
            return spark

        except Exception as e:
            logger.error(f"Failed to create SparkSession: {str(e)}")
            raise SparkSessionError(f"SparkSession creation failed: {str(e)}") from e

    @classmethod
    def stop_session(cls) -> None:
        """Stop the current SparkSession if it exists."""
        # Lazy import to avoid PySpark dependency at module level
        from pyspark.sql import SparkSession

        try:
            if cls._instance is not None:
                logger.info("Stopping SparkSession...")
                cls._instance.stop()
                cls._instance = None
            else:
                # Try to stop any active session
                existing_spark = SparkSession.getActiveSession()
                if existing_spark:
                    logger.info("Stopping active SparkSession...")
                    existing_spark.stop()
        except Exception as e:
            logger.warning(f"Error stopping SparkSession: {str(e)}")

    @classmethod
    def get_session(cls):
        """
        Get the current SparkSession instance.

        Returns:
            Current SparkSession or None if not created
        """
        # Lazy import to avoid PySpark dependency at module level
        from pyspark.sql import SparkSession

        if cls._instance is None:
            cls._instance = SparkSession.getActiveSession()
        return cls._instance

    @classmethod
    def configure_s3_access(cls, spark, s3_config) -> None:
        """
        Configure S3/MinIO access for the given SparkSession.

        Args:
            spark: SparkSession to configure
            s3_config: S3Config object with S3/MinIO settings
        """
        try:
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            hadoop_conf.set("fs.s3a.endpoint", s3_config.endpoint_url)
            hadoop_conf.set("fs.s3a.access.key", s3_config.access_key_id)
            hadoop_conf.set("fs.s3a.secret.key", s3_config.secret_access_key)
            hadoop_conf.set("fs.s3a.path.style.access",
                          str(s3_config.path_style_access).lower())
            hadoop_conf.set("fs.s3a.connection.ssl.enabled",
                          str(s3_config.use_ssl).lower())
            hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

            logger.info("S3/MinIO access configured for SparkSession")

        except Exception as e:
            logger.error(f"Failed to configure S3 access: {str(e)}")
            raise SparkSessionError(f"S3 configuration failed: {str(e)}") from e
