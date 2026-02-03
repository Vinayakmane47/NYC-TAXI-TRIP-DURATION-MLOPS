"""
Iceberg table management module.

This module provides functionality for creating, updating, and managing
Iceberg tables in MinIO/S3 storage, including schema management and
partition handling.
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from src.config import S3Config, SparkConfig
from src.utils.spark_utils import SparkSessionFactory
from src.lakehouse.table_registry import TableRegistry, TableMetadata
from src.exceptions import DataTransformationError, ConfigurationError

logger = logging.getLogger(__name__)


class IcebergManager:
    """
    Manager for Iceberg table operations.
    
    Handles creation, updates, and management of Iceberg tables in the data lake,
    supporting bronze, silver, and gold layer tables.
    """
    
    def __init__(
        self,
        spark_config: SparkConfig,
        s3_config: S3Config,
        catalog_name: str = "iceberg",
        registry: Optional[TableRegistry] = None
    ):
        """
        Initialize IcebergManager.
        
        Args:
            spark_config: Spark configuration
            s3_config: S3/MinIO configuration
            catalog_name: Trino catalog name for Iceberg tables
            registry: Optional TableRegistry instance
        """
        self.spark_config = spark_config
        self.s3_config = s3_config
        self.catalog_name = catalog_name
        self.registry = registry or TableRegistry()
        self.spark: Optional[SparkSession] = None
        self._setup_spark()
    
    def _setup_spark(self) -> None:
        """Setup Spark session with Iceberg support."""
        try:
            # Create or get Spark session
            self.spark = SparkSessionFactory.create_session(self.spark_config)
            
            # Configure S3 access
            SparkSessionFactory.configure_s3_access(self.spark, self.s3_config)
            
            # Configure Iceberg catalog
            self._configure_iceberg_catalog()
            
            logger.info("IcebergManager initialized with Spark session")
        except Exception as e:
            logger.error(f"Failed to setup Spark session: {e}")
            raise DataTransformationError(f"Spark setup failed: {e}") from e
    
    def _configure_iceberg_catalog(self) -> None:
        """Configure Iceberg catalog in Spark."""
        try:
            # Set Iceberg catalog properties
            warehouse_path = f"s3a://nyc-taxi-gold/iceberg/"
            
            # Configure Spark for Iceberg
            spark_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            
            # Set catalog properties
            catalog_props = {
                'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
                'spark.sql.catalog.iceberg.type': 'hadoop',
                'spark.sql.catalog.iceberg.warehouse': warehouse_path,
                'spark.sql.catalog.iceberg.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
                'spark.sql.catalog.iceberg.s3.endpoint': self.s3_config.endpoint_url,
                'spark.sql.catalog.iceberg.s3.access-key-id': self.s3_config.access_key_id,
                'spark.sql.catalog.iceberg.s3.secret-access-key': self.s3_config.secret_access_key,
                'spark.sql.catalog.iceberg.s3.path-style-access': str(self.s3_config.path_style_access).lower(),
            }
            
            for key, value in catalog_props.items():
                self.spark.conf.set(key, value)
            
            logger.info("Iceberg catalog configured")
        except Exception as e:
            logger.error(f"Failed to configure Iceberg catalog: {e}")
            raise ConfigurationError(f"Iceberg catalog configuration failed: {e}") from e
    
    def create_table_from_parquet(
        self,
        schema_name: str,
        table_name: str,
        parquet_path: str,
        partition_columns: Optional[List[str]] = None,
        table_location: Optional[str] = None
    ) -> TableMetadata:
        """
        Create Iceberg table from parquet files.
        
        Args:
            schema_name: Schema name (bronze, silver, or gold)
            table_name: Table name (e.g., 'trips')
            parquet_path: S3 path to parquet files
            partition_columns: Optional list of partition columns
            table_location: Optional custom table location (defaults to iceberg/{schema}/{table})
            
        Returns:
            TableMetadata object
        """
        try:
            logger.info(f"Creating Iceberg table {schema_name}.{table_name} from {parquet_path}")
            
            # Read parquet to infer schema
            df = self.spark.read.parquet(parquet_path)
            schema = df.schema
            
            # Determine table location
            if not table_location:
                # Default location: s3a://{bucket}/iceberg/{schema}/{table}
                bucket_map = {
                    'bronze': 'nyc-taxi-bronze',
                    'silver': 'nyc-taxi-silver',
                    'gold': 'nyc-taxi-gold'
                }
                bucket = bucket_map.get(schema_name, 'nyc-taxi-gold')
                table_location = f"s3a://{bucket}/iceberg/{schema_name}/{table_name}"
            
            # Create schema if it doesn't exist
            self._create_schema_if_not_exists(schema_name)
            
            # Check if table exists
            if self.table_exists(schema_name, table_name):
                logger.info(f"Table {schema_name}.{table_name} already exists, dropping and recreating")
                self.spark.sql(f"DROP TABLE IF EXISTS {self.catalog_name}.{schema_name}.{table_name}")
            
            # Write data to Iceberg table (this will create the table)
            writer = df.write \
                .format("iceberg") \
                .mode("overwrite") \
                .option("path", table_location)
            
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            
            writer.saveAsTable(f"{self.catalog_name}.{schema_name}.{table_name}")
            
            # Get partition spec
            partition_spec = {}
            if partition_columns:
                partition_spec = {col: "identity" for col in partition_columns}
            
            # Register table
            table_metadata = self.registry.register_table(
                schema_name=schema_name,
                table_name=table_name,
                table_location=table_location,
                catalog_name=self.catalog_name,
                partition_spec=partition_spec,
                schema_definition=self._schema_to_dict(schema)
            )
            
            logger.info(f"Successfully created Iceberg table {schema_name}.{table_name}")
            return table_metadata
            
        except Exception as e:
            logger.error(f"Failed to create Iceberg table: {e}")
            raise DataTransformationError(f"Iceberg table creation failed: {e}") from e
    
    def append_to_table(
        self,
        schema_name: str,
        table_name: str,
        parquet_path: str
    ) -> None:
        """
        Append data from parquet files to existing Iceberg table.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            parquet_path: S3 path to parquet files to append
        """
        try:
            logger.info(f"Appending data to {schema_name}.{table_name} from {parquet_path}")
            
            # Read parquet data
            df = self.spark.read.parquet(parquet_path)
            
            # Append to Iceberg table
            df.write \
                .format("iceberg") \
                .mode("append") \
                .insertInto(f"{self.catalog_name}.{schema_name}.{table_name}")
            
            # Update registry
            self.registry.update_table_metadata(schema_name, table_name)
            
            logger.info(f"Successfully appended data to {schema_name}.{table_name}")
            
        except Exception as e:
            logger.error(f"Failed to append to Iceberg table: {e}")
            raise DataTransformationError(f"Append to Iceberg table failed: {e}") from e
    
    def _create_schema_if_not_exists(self, schema_name: str) -> None:
        """Create schema in catalog if it doesn't exist."""
        try:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog_name}.{schema_name}")
            logger.debug(f"Schema {schema_name} created or already exists")
        except Exception as e:
            logger.warning(f"Failed to create schema {schema_name}: {e}")
    
    def _schema_to_dict(self, schema: StructType) -> Dict[str, Any]:
        """Convert Spark schema to dictionary representation."""
        return {
            'fields': [
                {
                    'name': field.name,
                    'type': str(field.dataType),
                    'nullable': field.nullable
                }
                for field in schema.fields
            ]
        }
    
    def table_exists(self, schema_name: str, table_name: str) -> bool:
        """
        Check if Iceberg table exists.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            tables = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}.{schema_name}").collect()
            table_names = [row.tableName for row in tables]
            return table_name in table_names
        except Exception as e:
            logger.warning(f"Error checking table existence: {e}")
            return False
    
    def get_table_info(self, schema_name: str, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about an Iceberg table.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            Dictionary with table information or None if not found
        """
        try:
            if not self.table_exists(schema_name, table_name):
                return None
            
            # Get table metadata
            table_metadata = self.registry.get_table(schema_name, table_name)
            if not table_metadata:
                return None
            
            # Get row count
            count = self.spark.sql(
                f"SELECT COUNT(*) as cnt FROM {self.catalog_name}.{schema_name}.{table_name}"
            ).collect()[0].cnt
            
            return {
                'schema_name': schema_name,
                'table_name': table_name,
                'location': table_metadata.table_location,
                'partition_spec': table_metadata.partition_spec,
                'row_count': count,
                'version': table_metadata.version
            }
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            return None
    
    def drop_table(self, schema_name: str, table_name: str, purge: bool = False) -> bool:
        """
        Drop an Iceberg table.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            purge: If True, delete data files (default: False)
            
        Returns:
            True if table was dropped, False otherwise
        """
        try:
            if not self.table_exists(schema_name, table_name):
                logger.warning(f"Table {schema_name}.{table_name} does not exist")
                return False
            
            purge_clause = "PURGE" if purge else ""
            self.spark.sql(
                f"DROP TABLE {purge_clause} {self.catalog_name}.{schema_name}.{table_name}"
            )
            
            # Unregister from registry
            self.registry.unregister_table(schema_name, table_name)
            
            logger.info(f"Dropped table {schema_name}.{table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to drop table: {e}")
            return False
    
    def stop(self) -> None:
        """Stop Spark session."""
        if self.spark:
            SparkSessionFactory.stop_session()
            self.spark = None
            logger.info("IcebergManager stopped")
