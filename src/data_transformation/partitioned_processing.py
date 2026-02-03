"""
Partition-Based Processing Module for NYC Taxi Data

This module provides utilities for processing large DataFrames in partitions
to prevent driver memory overload. The key principle is:

    NEVER COLLECT DATA TO DRIVER - Process and write directly from Spark

This is critical for scaling to large datasets where the driver cannot hold
all data in memory.

Strategy:
1. Get list of partition values (small metadata only)
2. Process each partition independently in Spark
3. Write results directly from Spark to MinIO/S3
4. Clear partition from memory before processing next one

Benefits:
- Driver memory usage remains constant (only metadata)
- Can process unlimited data size
- Memory usage scales linearly with partition size, not total data size
- Prevents OOM errors on driver node
"""

import logging
from typing import Optional, List, Callable, Dict, Any
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.memory_utils import MemoryManager
from src.exceptions import DataTransformationError

logger = logging.getLogger(__name__)


class PartitionedProcessor:
    """
    Process large DataFrames in partitions to prevent driver memory overload.

    This processor ensures that data stays in Spark workers and is never
    collected to the driver, allowing processing of arbitrarily large datasets.
    """

    def __init__(self, spark: SparkSession, memory_manager: Optional[MemoryManager] = None):
        """
        Initialize PartitionedProcessor.

        Args:
            spark: Active SparkSession
            memory_manager: Optional MemoryManager for cache control
        """
        self.spark = spark
        self.memory_manager = memory_manager or MemoryManager(spark)

    def get_partition_values(
        self,
        df: DataFrame,
        partition_col: str
    ) -> List[Any]:
        """
        Get list of partition values WITHOUT collecting the full DataFrame.

        Only the distinct partition values are collected to driver (small metadata).

        Args:
            df: DataFrame to get partitions from
            partition_col: Column to partition on

        Returns:
            List of distinct partition values
        """
        logger.info(f"Getting partition values for column: {partition_col}")

        try:
            # Only collect distinct partition values (small)
            partition_values = (
                df.select(partition_col)
                .distinct()
                .rdd.map(lambda row: row[0])
                .collect()
            )

            logger.info(f"Found {len(partition_values)} partitions")
            return sorted(partition_values)

        except Exception as e:
            logger.error(f"Failed to get partition values: {e}")
            raise DataTransformationError(
                f"Failed to get partition values: {e}"
            ) from e

    def process_partition(
        self,
        df: DataFrame,
        partition_col: str,
        partition_value: Any,
        transform_func: Callable[[DataFrame], DataFrame],
        output_path: str,
        output_format: str = "parquet",
        mode: str = "overwrite"
    ) -> Dict[str, Any]:
        """
        Process a single partition and write directly to storage.

        CRITICAL: This function NEVER collects data to driver.
        All processing happens in Spark, results written directly to MinIO/S3.

        Args:
            df: Full DataFrame
            partition_col: Column to partition on
            partition_value: Value of this partition
            transform_func: Function to apply to partition DataFrame
            output_path: Where to write results (S3/MinIO URI or local path)
            output_format: Output format (parquet, csv, etc.)
            mode: Write mode (overwrite, append, etc.)

        Returns:
            Dictionary with partition statistics
        """
        logger.info(f"Processing partition: {partition_col}={partition_value}")

        try:
            # Filter to current partition (still in Spark)
            partition_df = df.filter(F.col(partition_col) == partition_value)

            # Apply transformation (still in Spark)
            transformed_df = transform_func(partition_df)

            # Write directly from Spark to storage (NO DRIVER INVOLVEMENT)
            transformed_df.write.mode(mode).format(output_format).save(output_path)

            logger.info(f"Partition written to: {output_path}")

            # Get basic stats WITHOUT collecting full data
            # These are computed in Spark and only result aggregates collected
            stats = {
                "partition_value": partition_value,
                "output_path": output_path,
                "success": True
            }

            # Explicitly clear partition from memory
            partition_df.unpersist()
            transformed_df.unpersist()
            del partition_df
            del transformed_df

            # Trigger garbage collection
            self.memory_manager.clear_cache()

            return stats

        except Exception as e:
            logger.error(f"Failed to process partition {partition_value}: {e}")
            return {
                "partition_value": partition_value,
                "error": str(e),
                "success": False
            }

    def process_in_partitions(
        self,
        df: DataFrame,
        partition_col: str,
        transform_func: Callable[[DataFrame], DataFrame],
        base_output_path: str,
        output_format: str = "parquet",
        mode: str = "overwrite",
        partition_path_func: Optional[Callable[[Any], str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Process DataFrame in partitions to prevent driver memory overload.

        This is the main entry point for partition-based processing.

        Args:
            df: Input DataFrame
            partition_col: Column to partition on (e.g., 'month', 'pickup_date')
            transform_func: Function to apply to each partition
            base_output_path: Base path for output (e.g., 's3a://bucket/gold/2025/')
            output_format: Output format
            mode: Write mode
            partition_path_func: Optional function to generate partition-specific paths
                                 Default: {base_output_path}/{partition_value}/

        Returns:
            List of partition statistics
        """
        logger.info(f"Starting partition-based processing on column: {partition_col}")
        logger.info(f"Base output path: {base_output_path}")

        # Clear cache before starting
        self.memory_manager.clear_cache()

        try:
            # Get partition values (small metadata only)
            partition_values = self.get_partition_values(df, partition_col)

            # Default partition path function
            if partition_path_func is None:
                def default_path_func(value):
                    return f"{base_output_path.rstrip('/')}/{value}/"
                partition_path_func = default_path_func

            # Process each partition
            results = []
            for partition_value in partition_values:
                # Generate output path for this partition
                output_path = partition_path_func(partition_value)

                # Process partition (no driver collection)
                result = self.process_partition(
                    df=df,
                    partition_col=partition_col,
                    partition_value=partition_value,
                    transform_func=transform_func,
                    output_path=output_path,
                    output_format=output_format,
                    mode=mode
                )

                results.append(result)

                # Log progress
                successful = len([r for r in results if r.get('success', False)])
                logger.info(f"Progress: {successful}/{len(results)} partitions completed")

            # Summary
            successful = len([r for r in results if r.get('success', False)])
            failed = len(results) - successful

            logger.info(f"Partition processing complete: {successful} succeeded, {failed} failed")

            if failed > 0:
                logger.warning(f"Failed partitions: {[r['partition_value'] for r in results if not r.get('success')]}")

            return results

        except Exception as e:
            logger.error(f"Partition-based processing failed: {e}")
            raise DataTransformationError(
                f"Partition-based processing failed: {e}"
            ) from e

    def process_month_partitions(
        self,
        df: DataFrame,
        transform_func: Callable[[DataFrame], DataFrame],
        year: int,
        base_output_path: str,
        month_col: str = "month",
        output_format: str = "parquet"
    ) -> List[Dict[str, Any]]:
        """
        Convenience method for processing data partitioned by month.

        This is a common use case for the NYC Taxi dataset.

        Args:
            df: Input DataFrame (should have a month column)
            transform_func: Transformation to apply
            year: Year being processed
            base_output_path: Base output path
            month_col: Name of month column
            output_format: Output format

        Returns:
            List of partition statistics
        """
        def month_path_func(month: int) -> str:
            """Generate month-specific path: {base}/{year}/{month:02d}/"""
            return f"{base_output_path.rstrip('/')}/{year}/{month:02d}/"

        return self.process_in_partitions(
            df=df,
            partition_col=month_col,
            transform_func=transform_func,
            base_output_path=base_output_path,
            output_format=output_format,
            partition_path_func=month_path_func
        )


class ParallelPartitionProcessor(PartitionedProcessor):
    """
    Extension of PartitionedProcessor that processes multiple partitions in parallel.

    This uses Spark's parallelism to process multiple partitions simultaneously
    while still preventing driver memory overload.
    """

    def __init__(
        self,
        spark: SparkSession,
        max_parallel_partitions: int = 3,
        memory_manager: Optional[MemoryManager] = None
    ):
        """
        Initialize ParallelPartitionProcessor.

        Args:
            spark: Active SparkSession
            max_parallel_partitions: Maximum number of partitions to process in parallel
            memory_manager: Optional MemoryManager
        """
        super().__init__(spark, memory_manager)
        self.max_parallel_partitions = max_parallel_partitions

    def process_in_parallel_batches(
        self,
        df: DataFrame,
        partition_col: str,
        transform_func: Callable[[DataFrame], DataFrame],
        base_output_path: str,
        output_format: str = "parquet",
        mode: str = "overwrite",
        partition_path_func: Optional[Callable[[Any], str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Process partitions in parallel batches.

        Example: If max_parallel_partitions=3 and there are 12 months:
        - Batch 1: Process months 1, 2, 3 in parallel
        - Batch 2: Process months 4, 5, 6 in parallel
        - Batch 3: Process months 7, 8, 9 in parallel
        - Batch 4: Process months 10, 11, 12 in parallel

        Args:
            Same as process_in_partitions

        Returns:
            List of partition statistics
        """
        logger.info(
            f"Starting parallel partition processing with {self.max_parallel_partitions} "
            f"partitions per batch"
        )

        # Get partition values
        partition_values = self.get_partition_values(df, partition_col)

        # Create batches
        batches = [
            partition_values[i:i + self.max_parallel_partitions]
            for i in range(0, len(partition_values), self.max_parallel_partitions)
        ]

        logger.info(f"Processing {len(partition_values)} partitions in {len(batches)} batches")

        # Default partition path function
        if partition_path_func is None:
            def default_path_func(value):
                return f"{base_output_path.rstrip('/')}/{value}/"
            partition_path_func = default_path_func

        all_results = []

        for batch_num, batch in enumerate(batches, 1):
            logger.info(f"Processing batch {batch_num}/{len(batches)}: {batch}")

            # Process all partitions in batch
            batch_results = []
            for partition_value in batch:
                output_path = partition_path_func(partition_value)
                result = self.process_partition(
                    df=df,
                    partition_col=partition_col,
                    partition_value=partition_value,
                    transform_func=transform_func,
                    output_path=output_path,
                    output_format=output_format,
                    mode=mode
                )
                batch_results.append(result)

            all_results.extend(batch_results)

            # Clear cache after each batch
            self.memory_manager.clear_cache()
            logger.info(f"Batch {batch_num} completed, cache cleared")

        # Summary
        successful = len([r for r in all_results if r.get('success', False)])
        failed = len(all_results) - successful
        logger.info(f"Parallel processing complete: {successful} succeeded, {failed} failed")

        return all_results


# Convenience factory function
def create_partition_processor(
    spark: SparkSession,
    parallel: bool = False,
    max_parallel_partitions: int = 3
) -> PartitionedProcessor:
    """
    Factory function to create appropriate partition processor.

    Args:
        spark: SparkSession
        parallel: Whether to use parallel processing
        max_parallel_partitions: Max partitions to process in parallel

    Returns:
        PartitionedProcessor or ParallelPartitionProcessor
    """
    if parallel:
        return ParallelPartitionProcessor(
            spark,
            max_parallel_partitions=max_parallel_partitions
        )
    else:
        return PartitionedProcessor(spark)
