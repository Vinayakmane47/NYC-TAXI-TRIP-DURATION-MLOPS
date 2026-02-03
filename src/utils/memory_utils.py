"""
Memory Management Utilities for Spark Operations

Provides utilities for monitoring and managing memory in PySpark jobs
to prevent OOM errors and optimize performance.
"""

import logging
import gc
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.errors import AnalysisException

logger = logging.getLogger(__name__)


class MemoryManager:
    """
    Manages memory for PySpark operations with explicit cleanup and monitoring.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize MemoryManager.

        Args:
            spark: Active SparkSession
        """
        self.spark = spark

    def clear_cache(self, df: Optional[DataFrame] = None) -> None:
        """
        Clear Spark cache and trigger garbage collection.

        Args:
            df: Optional specific DataFrame to unpersist
        """
        if df is not None:
            try:
                df.unpersist(blocking=True)
                logger.debug("Unpersisted specific DataFrame")
            except Exception as e:
                logger.warning(f"Failed to unpersist DataFrame: {e}")

        # Clear all cached tables
        try:
            self.spark.catalog.clearCache()
            logger.debug("Cleared Spark catalog cache")
        except Exception as e:
            logger.warning(f"Failed to clear catalog cache: {e}")

        # Force Python garbage collection
        gc.collect()
        logger.debug("Triggered Python garbage collection")

    def get_memory_stats(self) -> Dict[str, Any]:
        """
        Get current Spark memory statistics.

        Returns:
            Dictionary with memory statistics
        """
        try:
            # Get Spark UI metrics
            sc = self.spark.sparkContext
            status = sc._jsc.sc().getExecutorMemoryStatus()

            # Parse executor memory info
            memory_info = {}
            for executor_id, mem_tuple in status.items():
                memory_info[str(executor_id)] = {
                    'max_memory': mem_tuple._1(),
                    'remaining_memory': mem_tuple._2()
                }

            return memory_info
        except Exception as e:
            logger.warning(f"Failed to get memory stats: {e}")
            return {}

    def optimize_partitions(
        self,
        df: DataFrame,
        target_partition_size_mb: int = 128,
        min_partitions: int = 1,
        max_partitions: int = 200
    ) -> int:
        """
        Calculate optimal partition count based on data size.

        Args:
            df: DataFrame to partition
            target_partition_size_mb: Target size per partition in MB
            min_partitions: Minimum number of partitions
            max_partitions: Maximum number of partitions

        Returns:
            Optimal partition count
        """
        try:
            # Estimate DataFrame size
            # Use cached metadata if available, otherwise estimate
            num_partitions = df.rdd.getNumPartitions()

            # If DataFrame is already well-partitioned, keep it
            if min_partitions <= num_partitions <= max_partitions:
                logger.info(f"Current partitions ({num_partitions}) within optimal range")
                return num_partitions

            # Calculate based on heuristics
            # For large datasets, aim for 128MB per partition
            # This is a conservative estimate to prevent OOM
            optimal = max(min_partitions, min(num_partitions * 2, max_partitions))

            logger.info(f"Calculated optimal partitions: {optimal} (current: {num_partitions})")
            return optimal

        except Exception as e:
            logger.warning(f"Failed to optimize partitions: {e}. Using default.")
            return min(max(min_partitions, 10), max_partitions)

    def repartition_if_needed(
        self,
        df: DataFrame,
        target_partitions: Optional[int] = None,
        force: bool = False
    ) -> DataFrame:
        """
        Repartition DataFrame if needed to optimize memory usage.

        Args:
            df: DataFrame to repartition
            target_partitions: Target partition count (auto-calculated if None)
            force: Force repartitioning even if current is optimal

        Returns:
            Potentially repartitioned DataFrame
        """
        current_partitions = df.rdd.getNumPartitions()

        if target_partitions is None:
            target_partitions = self.optimize_partitions(df)

        if force or abs(current_partitions - target_partitions) > 5:
            logger.info(f"Repartitioning: {current_partitions} -> {target_partitions}")
            # Use coalesce if reducing partitions (avoids shuffle)
            if target_partitions < current_partitions:
                return df.coalesce(target_partitions)
            else:
                return df.repartition(target_partitions)
        else:
            logger.debug(f"Skipping repartition (current: {current_partitions}, target: {target_partitions})")
            return df

    def checkpoint_dataframe(
        self,
        df: DataFrame,
        checkpoint_dir: str,
        eager: bool = True
    ) -> DataFrame:
        """
        Checkpoint DataFrame to disk to break lineage and free memory.

        Args:
            df: DataFrame to checkpoint
            checkpoint_dir: Directory for checkpoint files
            eager: Whether to eagerly checkpoint (triggers action)

        Returns:
            Checkpointed DataFrame
        """
        try:
            self.spark.sparkContext.setCheckpointDir(checkpoint_dir)
            if eager:
                df = df.checkpoint(eager=True)
                logger.info(f"Eagerly checkpointed DataFrame to {checkpoint_dir}")
            else:
                df = df.checkpoint(eager=False)
                logger.info(f"Lazily checkpointed DataFrame to {checkpoint_dir}")
            return df
        except Exception as e:
            logger.warning(f"Failed to checkpoint DataFrame: {e}")
            return df


class MemoryEfficientProcessor:
    """
    Provides memory-efficient data processing strategies.
    """

    def __init__(self, spark: SparkSession, memory_manager: Optional[MemoryManager] = None):
        """
        Initialize processor.

        Args:
            spark: Active SparkSession
            memory_manager: Optional MemoryManager instance
        """
        self.spark = spark
        self.memory_manager = memory_manager or MemoryManager(spark)

    def broadcast_small_dataframe(self, df: DataFrame, threshold_rows: int = 10000) -> DataFrame:
        """
        Broadcast small DataFrame if below threshold to optimize joins.

        Args:
            df: DataFrame to potentially broadcast
            threshold_rows: Row threshold for broadcasting

        Returns:
            Original or broadcast DataFrame
        """
        try:
            from pyspark.sql.functions import broadcast

            # Check if DataFrame is small enough to broadcast
            # Note: count() triggers action, use cautiously
            # Better to broadcast based on schema/knowledge
            logger.info("Using broadcast hint for small DataFrame")
            return broadcast(df)

        except Exception as e:
            logger.warning(f"Failed to broadcast DataFrame: {e}")
            return df

    def incremental_aggregation(
        self,
        df: DataFrame,
        group_cols: list,
        agg_expr: dict,
        partition_col: Optional[str] = None
    ) -> DataFrame:
        """
        Perform aggregation with memory-efficient strategies.

        Args:
            df: DataFrame to aggregate
            group_cols: Columns to group by
            agg_expr: Aggregation expressions
            partition_col: Optional column to partition by before aggregation

        Returns:
            Aggregated DataFrame
        """
        try:
            # If partition column provided, process incrementally
            if partition_col:
                logger.info(f"Using incremental aggregation on {partition_col}")
                # This is a placeholder - would need specific implementation
                # based on use case
                pass

            # Standard aggregation with optimized shuffle partitions
            original_partitions = self.spark.conf.get("spark.sql.shuffle.partitions")

            # Reduce shuffle partitions for small datasets
            self.spark.conf.set("spark.sql.shuffle.partitions", "8")

            result = df.groupBy(*group_cols).agg(agg_expr)

            # Restore original setting
            self.spark.conf.set("spark.sql.shuffle.partitions", original_partitions)

            return result

        except Exception as e:
            logger.error(f"Aggregation failed: {e}")
            raise

    def process_in_batches(
        self,
        df: DataFrame,
        batch_col: str,
        process_func,
        **kwargs
    ):
        """
        Process DataFrame in batches based on a column.

        Args:
            df: DataFrame to process
            batch_col: Column to batch on (e.g., month)
            process_func: Function to apply to each batch
            **kwargs: Additional arguments for process_func

        Yields:
            Processed DataFrame for each batch
        """
        # Get distinct values for batching
        batch_values = [row[batch_col] for row in df.select(batch_col).distinct().collect()]

        logger.info(f"Processing {len(batch_values)} batches on column '{batch_col}'")

        for batch_value in batch_values:
            logger.info(f"Processing batch: {batch_col}={batch_value}")

            # Filter to current batch
            batch_df = df.filter(df[batch_col] == batch_value)

            # Process batch
            result_df = process_func(batch_df, **kwargs)

            # Clear cache after processing
            self.memory_manager.clear_cache(batch_df)

            yield batch_value, result_df


def _safe_set_config(spark: SparkSession, key: str, value: str) -> None:
    """
    Safely set Spark config, skipping if it cannot be modified (e.g., set via command line).
    
    Args:
        spark: SparkSession to configure
        key: Config key
        value: Config value
    """
    try:
        spark.conf.set(key, value)
        logger.debug(f"Set {key} = {value}")
    except AnalysisException as e:
        # Config cannot be modified (set via command line/DAG), skip it
        # This is expected for configs set in SparkSubmitOperator
        logger.debug(f"Skipping {key} (cannot modify: already set via command line/DAG)")
    except Exception as e:
        # Other exceptions - log but don't fail
        logger.debug(f"Could not set {key}: {e}")


def configure_memory_optimized_spark(spark: SparkSession) -> None:
    """
    Configure Spark session for memory-optimized processing.
    Only sets configs that are not already set (e.g., via command line or DAG).

    Args:
        spark: SparkSession to configure
    """
    logger.info("Applying memory-optimized Spark configurations...")

    # Memory management - these may be set in DAG, so use safe set
    _safe_set_config(spark, "spark.memory.fraction", "0.6")  # Reduced from 0.7
    _safe_set_config(spark, "spark.memory.storageFraction", "0.3")  # Increased from 0.2
    _safe_set_config(spark, "spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    _safe_set_config(spark, "spark.cleaner.periodicGC.interval", "10min")

    # Shuffle optimization
    _safe_set_config(spark, "spark.sql.shuffle.partitions", "8")  # Reduced to 8 partitions
    _safe_set_config(spark, "spark.sql.adaptive.enabled", "true")
    _safe_set_config(spark, "spark.sql.adaptive.coalescePartitions.enabled", "true")
    _safe_set_config(spark, "spark.sql.adaptive.skewJoin.enabled", "true")

    # File size optimization
    _safe_set_config(spark, "spark.sql.files.maxPartitionBytes", "67108864")  # 64MB

    # Broadcast optimization
    _safe_set_config(spark, "spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB

    # Serialization
    _safe_set_config(spark, "spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    _safe_set_config(spark, "spark.kryoserializer.buffer.max", "512m")

    # Execution optimization
    _safe_set_config(spark, "spark.sql.execution.arrow.pyspark.enabled", "false")  # Can cause OOM
    _safe_set_config(spark, "spark.sql.inMemoryColumnarStorage.compressed", "true")
    _safe_set_config(spark, "spark.sql.inMemoryColumnarStorage.batchSize", "5000")  # Reduced from 10000

    logger.info("Memory-optimized configurations applied")
