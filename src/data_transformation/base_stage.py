"""
Abstract base class for data transformation stages.

This module provides a base class that eliminates code duplication between
preprocessing and feature engineering stages by providing common functionality
for SparkSession management, S3/local I/O, and data processing patterns.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from src.utils.spark_utils import SparkSessionFactory
from src.utils.s3_utils import S3Client, DataLakeManager
from src.exceptions import DataTransformationError, SparkSessionError, S3StorageError

logger = logging.getLogger(__name__)


class BaseDataTransformationStage(ABC):
    """
    Abstract base class for data transformation stages.

    Provides common functionality for:
    - SparkSession creation and management
    - S3/MinIO configuration
    - Directory setup
    - File discovery (S3 and local)
    - Data reading and writing
    - Resource cleanup
    """

    def __init__(self, config, use_existing_spark: Optional[SparkSession] = None):
        """
        Initialize the transformation stage.

        Args:
            config: Configuration object (DataPreprocessingConfig or FeatureEngineeringConfig)
            use_existing_spark: Optional existing SparkSession to reuse
        """
        self.config = config
        self.s3_client: Optional[S3Client] = None
        self.data_lake_manager: Optional[DataLakeManager] = None

        # Create or use existing SparkSession
        if use_existing_spark:
            logger.info("Using provided SparkSession")
            self.spark = use_existing_spark
            self._owns_spark = False
        else:
            self.spark = self._create_spark_session()
            self._owns_spark = True

        # Configure S3 if enabled
        if self.config.use_s3 and self.config.s3:
            self._configure_s3()

        # Setup directories
        self._setup_directories()

    def _create_spark_session(self) -> SparkSession:
        """
        Create and configure SparkSession using factory.

        Returns:
            Configured SparkSession

        Raises:
            SparkSessionError: If SparkSession creation fails
        """
        logger.info(f"Creating SparkSession: {self.config.spark.app_name}")

        try:
            spark = SparkSessionFactory.create_session(
                spark_config=self.config.spark,
                force_new=False
            )

            # Configure S3 access if needed
            if self.config.use_s3 and self.config.s3:
                SparkSessionFactory.configure_s3_access(spark, self.config.s3)

            return spark

        except Exception as e:
            logger.error(f"Failed to create SparkSession: {str(e)}")
            raise SparkSessionError(f"SparkSession creation failed: {str(e)}") from e

    def _configure_s3(self) -> None:
        """Configure S3/MinIO client and data lake manager."""
        try:
            logger.info("Configuring S3/MinIO client...")
            self.s3_client = S3Client(self.config.s3)
            self.data_lake_manager = DataLakeManager(self.s3_client)

            # Ensure required buckets exist
            self._ensure_buckets_exist()

            logger.info("S3/MinIO configuration completed")

        except Exception as e:
            logger.error(f"Failed to configure S3: {str(e)}")
            raise S3StorageError(f"S3 configuration failed: {str(e)}") from e

    @abstractmethod
    def _ensure_buckets_exist(self) -> None:
        """
        Ensure required S3 buckets exist.

        Must be implemented by subclasses to specify which buckets are needed.
        """
        pass

    @abstractmethod
    def _setup_directories(self) -> None:
        """
        Create necessary directories if they don't exist.

        Must be implemented by subclasses to specify which directories are needed.
        """
        pass

    @abstractmethod
    def _get_input_bucket(self) -> Optional[str]:
        """
        Get the input bucket name for S3 operations.

        Returns:
            Bucket name or None if not using S3
        """
        pass

    @abstractmethod
    def _get_output_bucket(self) -> Optional[str]:
        """
        Get the output bucket name for S3 operations.

        Returns:
            Bucket name or None if not using S3
        """
        pass

    @abstractmethod
    def _get_input_directory(self) -> Path:
        """
        Get the input directory path for local operations.

        Returns:
            Path to input directory
        """
        pass

    @abstractmethod
    def _get_output_directory(self) -> Path:
        """
        Get the output directory path for local operations.

        Returns:
            Path to output directory
        """
        pass

    @abstractmethod
    def _get_s3_prefix_pattern(self, year: int, month: int) -> str:
        """
        Get the S3 prefix pattern for file discovery.

        Args:
            year: Year
            month: Month (1-12)

        Returns:
            S3 prefix string (e.g., "bronze/2023/01/")
        """
        pass

    def _get_data_files(
        self,
        year: int,
        months: Optional[List[int]] = None,
        layer: str = "bronze"
    ) -> List[str]:
        """
        Get list of data files to process (S3 paths or local paths).

        Args:
            year: Year to process
            months: List of months to process. If None or empty, process all months.
            layer: Data layer name (bronze, silver, gold)

        Returns:
            List of file paths (S3 URIs if use_s3, local paths otherwise)
        """
        if self.config.use_s3 and self._get_input_bucket() and self.s3_client:
            return self._get_s3_files(year, months, layer)
        else:
            return self._get_local_files(year, months, layer)

    def _get_s3_files(self, year: int, months: Optional[List[int]], layer: str) -> List[str]:
        """Get files from S3."""
        files = []
        input_bucket = self._get_input_bucket()

        # Bronze layer: month-specific directories (bronze/2025/01/, bronze/2025/02/, etc.)
        # Silver/Gold layers: combined directory (silver/2025/, gold/2025/)
        if layer == "bronze":
            # Bronze: read from month-specific directories
            if months is None or len(months) == 0:
                months = list(range(1, 13))
            
            for month in months:
                prefix = self._get_s3_prefix_pattern(year, month)
                s3_files = self.s3_client.list_objects(input_bucket, prefix)
                parquet_files = [
                    f"s3a://{input_bucket}/{f}"
                    for f in s3_files if f.endswith('.parquet')
                ]
                files.extend(parquet_files)
        else:
            # Silver/Gold: read from month-specific directories if processing single month
            # Otherwise read from combined directory
            if months and len(months) == 1:
                # Single month: read from month-specific directory (silver/2025/01/, gold/2025/01/, etc.)
                month = months[0]
                prefix = f"{layer}/{year}/{month:02d}/"
                s3_files = self.s3_client.list_objects(input_bucket, prefix)
                parquet_files = [
                    f"s3a://{input_bucket}/{f}"
                    for f in s3_files if f.endswith('.parquet')
                ]
                files.extend(parquet_files)
            else:
                # Multiple months: read from combined directory (silver/2025/ or gold/2025/)
                prefix = f"{layer}/{year}/"
                s3_files = self.s3_client.list_objects(input_bucket, prefix)
                parquet_files = [
                    f"s3a://{input_bucket}/{f}"
                    for f in s3_files if f.endswith('.parquet')
                ]
                files.extend(parquet_files)

        logger.info(f"Found {len(files)} S3 files to process from {layer} layer")
        return files

    def _get_local_files(self, year: int, months: Optional[List[int]], layer: str = "bronze") -> List[str]:
        """Get files from local filesystem."""
        data_dir = self._get_input_directory() / str(year)

        if not data_dir.exists():
            logger.warning(f"Data directory not found: {data_dir}")
            return []

        files = []
        
        # Bronze layer: month-specific directories (bronze/2025/01/, bronze/2025/02/, etc.)
        # Silver/Gold layers: combined directory (silver/2025/, gold/2025/ with partitioned files)
        if layer == "bronze":
            # Bronze: month-specific directories
            if months is None or len(months) == 0:
                # Process all months
                for month_dir in sorted(data_dir.iterdir()):
                    if month_dir.is_dir():
                        parquet_files = list(month_dir.glob("*.parquet"))
                        files.extend([str(f) for f in parquet_files])
            else:
                # Process specific months
                for month in months:
                    month_dir = data_dir / f"{month:02d}"
                    if month_dir.exists():
                        # Support both part_*.parquet (new naming) and part-*.parquet (Spark default)
                        parquet_files = list(month_dir.glob("part_*.parquet"))
                        if not parquet_files:
                            parquet_files = list(month_dir.glob("part-*.parquet"))
                        files.extend([str(f) for f in parquet_files])
                    else:
                        logger.warning(f"Month directory not found: {month_dir}")
        else:
            # Silver/Gold: read from month-specific directories if processing single month
            # Otherwise read from combined directory
            if months and len(months) == 1:
                # Single month: read from month-specific directory (silver/2025/01/, gold/2025/01/, etc.)
                month = months[0]
                month_dir = data_dir / f"{month:02d}"
                if month_dir.exists():
                    # Support both part_*.parquet (new naming) and part-*.parquet (Spark default)
                    parquet_files = list(month_dir.glob("part_*.parquet"))
                    if not parquet_files:
                        parquet_files = list(month_dir.glob("part-*.parquet"))
                    files.extend([str(f) for f in parquet_files])
                    logger.info(f"Reading from month-specific {layer} directory: {month_dir}")
                else:
                    logger.warning(f"Month directory not found: {month_dir}")
            else:
                # Multiple months: combined directory with partitioned files
                # Support both part_*.parquet (new naming) and part-*.parquet (Spark default)
                parquet_files = list(data_dir.glob("part_*.parquet"))
                if not parquet_files:
                    parquet_files = list(data_dir.glob("part-*.parquet"))
                files.extend([str(f) for f in parquet_files])
                logger.info(f"Reading from combined {layer} directory: {data_dir}")

        logger.info(f"Found {len(files)} local files to process from {layer} layer")
        return files

    def _read_data(self, file_paths: List[str]) -> DataFrame:
        """
        Read parquet files into Spark DataFrame (supports both S3 and local paths).

        Args:
            file_paths: List of file paths to read (S3 URIs or local paths)

        Returns:
            Spark DataFrame

        Raises:
            DataTransformationError: If reading data fails
        """
        if not file_paths:
            raise DataTransformationError("No files to read")

        try:
            logger.info(f"Reading {len(file_paths)} parquet files...")

            # Read with optimized settings
            df = (
                self.spark.read
                .option("mergeSchema", "true")
                .parquet(*file_paths)
            )

            # Skip row count to prevent OOM - just log file count
            logger.info(f"Read data from {len(file_paths)} parquet files")

            return df

        except Exception as e:
            logger.error(f"Failed to read data: {str(e)}")
            raise DataTransformationError(f"Data reading failed: {str(e)}") from e

    def _save_data(
        self,
        df: DataFrame,
        output_path: Union[Path, str],
        layer: str = "silver"
    ) -> str:
        """
        Save DataFrame to parquet format with memory-efficient write (supports S3 and local).

        Args:
            df: Spark DataFrame to save
            output_path: Path to save the data (local Path or relative S3 path)
            layer: Data layer name (silver, gold)

        Returns:
            Final save path (S3 URI or local path)

        Raises:
            DataTransformationError: If saving data fails
        """
        try:
            # Determine if saving to S3 or local
            if self.config.use_s3 and self._get_output_bucket():
                # Save to S3
                output_bucket = self._get_output_bucket()
                # output_path already includes the layer directory (e.g., "silver/2025/...")
                # so we don't need to add layer prefix again
                s3_path = f"s3a://{output_bucket}/{output_path}"
                save_path = s3_path
                logger.info(f"Saving processed data to S3: {s3_path}")
            else:
                # Save locally
                if isinstance(output_path, str):
                    output_path = Path(output_path)
                save_path = str(output_path)
                logger.info(f"Saving processed data to: {output_path}")
                # output_path is now a directory, not a file
                output_path.mkdir(parents=True, exist_ok=True)

            # Unpersist any cached dataframes to free memory
            try:
                df.unpersist(blocking=True)
            except Exception:
                pass  # DataFrame was not cached, ignore

            # Use coalesce instead of repartition for write (avoids shuffle)
            write_partitions = getattr(self.config.processing, 'write_partitions', None)

            if write_partitions is None:
                # Use a fixed reasonable default instead of counting rows (prevents OOM)
                # For monthly batches, 10 partitions is usually sufficient
                write_partitions = 10
                logger.info(f"Using default write_partitions: {write_partitions} (count() skipped to prevent OOM)")

            logger.info(f"Coalescing to {write_partitions} partitions for write")
            df = df.coalesce(write_partitions)

            # Write DataFrame with optimized settings
            writer = df.write.mode(self.config.output.mode)

            if self.config.output.format == "parquet":
                writer = (
                    writer
                    .option("compression", self.config.output.compression)
                    .option("parquet.block.size", "134217728")  # 128MB blocks
                    .option("parquet.page.size", "1048576")  # 1MB pages
                )
                writer.parquet(save_path)
            else:
                raise DataTransformationError(
                    f"Unsupported output format: {self.config.output.format}"
                )

            # Rename partition files to meaningful names (part_0.parquet, part_1.parquet, etc.)
            self._rename_partition_files(save_path, output_path)

            logger.info(f"Data saved successfully to: {save_path}")
            return save_path

        except Exception as e:
            logger.error(f"Failed to save data: {str(e)}")
            raise DataTransformationError(f"Data saving failed: {str(e)}") from e

    def _rename_partition_files(self, save_path: str, output_path: Union[Path, str]) -> None:
        """
        Rename Spark partition files from part-00000-xxx-c000.snappy.parquet to part_0.parquet, part_1.parquet, etc.
        
        Args:
            save_path: Full path where data was saved (S3 URI or local path)
            output_path: Relative output path (for S3 key construction)
        """
        try:
            import re
            from pathlib import Path as PathLib
            
            if self.config.use_s3 and self._get_output_bucket():
                # S3: Rename files using S3 client
                output_bucket = self._get_output_bucket()
                if isinstance(output_path, str):
                    prefix = output_path
                else:
                    prefix = str(output_path)
                
                # List all parquet files in the directory (excluding _SUCCESS and _committed files)
                s3_files = self.s3_client.list_objects(output_bucket, prefix)
                part_files = [
                    f for f in s3_files 
                    if f.endswith('.parquet') and (f.startswith('part-') or f.startswith('part_')) and '_SUCCESS' not in f
                ]
                
                # Sort files to maintain order
                part_files.sort()
                
                # Rename each file using boto3 copy (S3 doesn't support direct rename)
                for idx, old_key in enumerate(part_files):
                    # Extract directory path from old_key
                    dir_path = '/'.join(old_key.split('/')[:-1]) if '/' in old_key else ''
                    new_key = f"{dir_path}/part_{idx}.parquet" if dir_path else f"part_{idx}.parquet"
                    
                    try:
                        # Copy object to new key
                        copy_source = {'Bucket': output_bucket, 'Key': old_key}
                        self.s3_client.client.copy_object(
                            CopySource=copy_source,
                            Bucket=output_bucket,
                            Key=new_key
                        )
                        # Delete old object
                        self.s3_client.delete_object(output_bucket, old_key)
                        logger.debug(f"Renamed S3 object {old_key} to {new_key}")
                    except Exception as e:
                        logger.warning(f"Failed to rename {old_key} to {new_key}: {e}")
                
                logger.info(f"Renamed {len(part_files)} partition files in S3")
            else:
                # Local filesystem: Rename files directly
                if isinstance(output_path, str):
                    output_dir = PathLib(output_path)
                else:
                    output_dir = PathLib(output_path)
                
                if not output_dir.exists():
                    logger.warning(f"Output directory does not exist: {output_dir}")
                    return
                
                # Find all part-*.parquet files
                part_files = sorted(output_dir.glob("part-*.parquet"))
                
                # Rename each file
                for idx, old_file in enumerate(part_files):
                    new_file = output_dir / f"part_{idx}.parquet"
                    if old_file != new_file:
                        old_file.rename(new_file)
                        logger.debug(f"Renamed {old_file.name} to {new_file.name}")
                
                logger.info(f"Renamed {len(part_files)} partition files locally")
                
        except Exception as e:
            logger.warning(f"Failed to rename partition files: {e}. Files will keep Spark's default naming.")
            # Don't raise - this is a nice-to-have feature

    @abstractmethod
    def _transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply transformation logic to the DataFrame.

        This is the core transformation logic that must be implemented by subclasses.

        Args:
            df: Input DataFrame

        Returns:
            Transformed DataFrame
        """
        pass

    def stop(self) -> None:
        """Stop the SparkSession if this instance owns it."""
        if self._owns_spark and self.spark:
            logger.info("Stopping SparkSession...")
            SparkSessionFactory.stop_session()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        self.stop()
        return False
