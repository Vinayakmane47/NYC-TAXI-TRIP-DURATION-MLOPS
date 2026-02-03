"""
Catalog manager for Trino Iceberg catalog operations.

This module provides functionality for managing Trino catalogs, schemas,
and registering Iceberg tables.
"""

import logging
from typing import List, Optional, Dict, Any

from src.trino_integration.trino_client import TrinoClient
from src.lakehouse.table_registry import TableRegistry
from src.exceptions import ConfigurationError, DataTransformationError

logger = logging.getLogger(__name__)


class CatalogManager:
    """
    Manager for Trino catalog operations.
    
    Handles schema creation, table registration, and catalog management
    for Iceberg tables.
    """
    
    def __init__(
        self,
        trino_client: TrinoClient,
        registry: Optional[TableRegistry] = None
    ):
        """
        Initialize CatalogManager.
        
        Args:
            trino_client: TrinoClient instance
            registry: Optional TableRegistry for table metadata
        """
        self.trino_client = trino_client
        self.registry = registry
    
    def create_schema(
        self,
        schema_name: str,
        catalog_name: Optional[str] = None,
        if_not_exists: bool = True
    ) -> bool:
        """
        Create a schema in the catalog.
        
        Args:
            schema_name: Schema name
            catalog_name: Catalog name (defaults to client's catalog)
            if_not_exists: If True, use CREATE SCHEMA IF NOT EXISTS
            
        Returns:
            True if schema was created or already exists
        """
        try:
            catalog = catalog_name or self.trino_client.catalog
            if_clause = "IF NOT EXISTS" if if_not_exists else ""
            
            query = f"CREATE SCHEMA {if_clause} {catalog}.{schema_name}"
            self.trino_client.execute_update(query)
            
            logger.info(f"Schema {catalog}.{schema_name} created or already exists")
            return True
        except Exception as e:
            logger.error(f"Failed to create schema {schema_name}: {e}")
            raise DataTransformationError(f"Schema creation failed: {e}") from e
    
    def schema_exists(
        self,
        schema_name: str,
        catalog_name: Optional[str] = None
    ) -> bool:
        """
        Check if a schema exists.
        
        Args:
            schema_name: Schema name
            catalog_name: Catalog name
            
        Returns:
            True if schema exists
        """
        try:
            schemas = self.trino_client.list_schemas(catalog_name)
            return schema_name in schemas
        except Exception as e:
            logger.warning(f"Error checking schema existence: {e}")
            return False
    
    def table_exists(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        catalog_name: Optional[str] = None
    ) -> bool:
        """
        Check if a table exists.
        
        Args:
            table_name: Table name
            schema_name: Schema name
            catalog_name: Catalog name
            
        Returns:
            True if table exists
        """
        try:
            tables = self.trino_client.list_tables(schema_name, catalog_name)
            return table_name in tables
        except Exception as e:
            logger.warning(f"Error checking table existence: {e}")
            return False
    
    def register_table_from_registry(
        self,
        schema_name: str,
        table_name: str
    ) -> bool:
        """
        Register a table from the registry (verify it's accessible in Trino).
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            True if table is accessible
        """
        if not self.registry:
            logger.warning("No registry provided, skipping registration")
            return False
        
        try:
            # Get table metadata from registry
            table_metadata = self.registry.get_table(schema_name, table_name)
            if not table_metadata:
                logger.warning(f"Table {schema_name}.{table_name} not found in registry")
                return False
            
            # Verify table is accessible in Trino
            if self.table_exists(table_name, schema_name, table_metadata.catalog_name):
                logger.info(f"Table {schema_name}.{table_name} is accessible in Trino")
                return True
            else:
                logger.warning(f"Table {schema_name}.{table_name} not found in Trino catalog")
                return False
                
        except Exception as e:
            logger.error(f"Failed to register table: {e}")
            return False
    
    def setup_catalog_schemas(
        self,
        schemas: List[str],
        catalog_name: Optional[str] = None
    ) -> Dict[str, bool]:
        """
        Setup multiple schemas in the catalog.
        
        Args:
            schemas: List of schema names to create
            catalog_name: Catalog name
            
        Returns:
            Dictionary mapping schema names to success status
        """
        results = {}
        for schema in schemas:
            try:
                results[schema] = self.create_schema(schema, catalog_name)
            except Exception as e:
                logger.error(f"Failed to create schema {schema}: {e}")
                results[schema] = False
        return results
    
    def get_table_info(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        catalog_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get information about a table.
        
        Args:
            table_name: Table name
            schema_name: Schema name
            catalog_name: Catalog name
            
        Returns:
            Dictionary with table information or None if not found
        """
        try:
            if not self.table_exists(table_name, schema_name, catalog_name):
                return None
            
            schema = schema_name or self.trino_client.schema
            catalog = catalog_name or self.trino_client.catalog
            
            # Get row count
            count = self.trino_client.get_table_count(table_name, schema, catalog)
            
            # Get sample columns
            sample = self.trino_client.query_table(table_name, schema, catalog, limit=1)
            columns = list(sample[0].keys()) if sample else []
            
            return {
                'catalog': catalog,
                'schema': schema,
                'table': table_name,
                'full_name': f"{catalog}.{schema}.{table_name}",
                'row_count': count,
                'columns': columns
            }
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            return None
    
    def create_view(
        self,
        view_name: str,
        query: str,
        schema_name: Optional[str] = None,
        catalog_name: Optional[str] = None,
        replace: bool = False
    ) -> bool:
        """
        Create a view in the catalog.
        
        Args:
            view_name: View name
            query: SELECT query for the view
            schema_name: Schema name
            catalog_name: Catalog name
            replace: If True, use CREATE OR REPLACE VIEW
            
        Returns:
            True if view was created
        """
        try:
            schema = schema_name or self.trino_client.schema
            catalog = catalog_name or self.trino_client.catalog
            
            replace_clause = "OR REPLACE" if replace else ""
            query_sql = f"CREATE {replace_clause} VIEW {catalog}.{schema}.{view_name} AS {query}"
            
            self.trino_client.execute_update(query_sql)
            logger.info(f"View {catalog}.{schema}.{view_name} created")
            return True
        except Exception as e:
            logger.error(f"Failed to create view: {e}")
            raise DataTransformationError(f"View creation failed: {e}") from e
