"""
Trino client wrapper for SQL queries.

This module provides a Python client for executing SQL queries against
Trino, which provides SQL access to Iceberg tables.
"""

import logging
from typing import List, Dict, Any, Optional
from contextlib import contextmanager

try:
    from trino.dbapi import connect as trino_connect
    from trino.exceptions import TrinoQueryError
    TRINO_AVAILABLE = True
except ImportError:
    TRINO_AVAILABLE = False
    TrinoQueryError = Exception

from src.exceptions import ConfigurationError, DataTransformationError

logger = logging.getLogger(__name__)


class TrinoClient:
    """
    Trino client wrapper for executing SQL queries.
    
    Provides connection management and query execution with error handling.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8080,
        user: str = "trino",
        catalog: str = "iceberg",
        schema: str = "default",
        password: Optional[str] = None,
        http_scheme: str = "http"
    ):
        """
        Initialize TrinoClient.
        
        Args:
            host: Trino coordinator host
            port: Trino coordinator port
            user: Trino user
            catalog: Default catalog name
            schema: Default schema name
            password: Optional password for authentication
            http_scheme: HTTP scheme (http or https)
        """
        if not TRINO_AVAILABLE:
            raise ConfigurationError(
                "Trino client library not available. Install with: pip install trino"
            )
        
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self.password = password
        self.http_scheme = http_scheme
        self.connection = None
    
    def _get_connection(self):
        """Get or create Trino connection."""
        if self.connection is None or self.connection.is_closed():
            try:
                # Import auth module if password is provided
                if self.password:
                    from trino.auth import BasicAuthentication
                    auth = BasicAuthentication(self.user, self.password)
                else:
                    auth = None
                
                self.connection = trino_connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    catalog=self.catalog,
                    schema=self.schema,
                    http_scheme=self.http_scheme,
                    auth=auth
                )
                logger.debug(f"Connected to Trino at {self.host}:{self.port}")
            except Exception as e:
                logger.error(f"Failed to connect to Trino: {e}")
                raise ConfigurationError(f"Trino connection failed: {e}") from e
        return self.connection
    
    @contextmanager
    def _get_cursor(self):
        """Get cursor with automatic cleanup."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
    
    def execute_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query string
            parameters: Optional query parameters
            
        Returns:
            List of dictionaries representing rows
            
        Raises:
            DataTransformationError: If query execution fails
        """
        try:
            with self._get_cursor() as cursor:
                logger.debug(f"Executing query: {query[:100]}...")
                cursor.execute(query, parameters)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                # Fetch all results
                rows = cursor.fetchall()
                
                # Convert to list of dictionaries
                results = [dict(zip(columns, row)) for row in rows]
                
                logger.debug(f"Query returned {len(results)} rows")
                return results
                
        except TrinoQueryError as e:
            logger.error(f"Trino query error: {e}")
            raise DataTransformationError(f"Query execution failed: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error executing query: {e}")
            raise DataTransformationError(f"Query execution failed: {e}") from e
    
    def execute_update(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Execute an update/insert/delete query.
        
        Args:
            query: SQL query string
            parameters: Optional query parameters
            
        Returns:
            Number of affected rows
        """
        try:
            with self._get_cursor() as cursor:
                logger.debug(f"Executing update: {query[:100]}...")
                cursor.execute(query, parameters)
                return cursor.rowcount
        except TrinoQueryError as e:
            logger.error(f"Trino query error: {e}")
            raise DataTransformationError(f"Update execution failed: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error executing update: {e}")
            raise DataTransformationError(f"Update execution failed: {e}") from e
    
    def query_table(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        catalog_name: Optional[str] = None,
        where_clause: Optional[str] = None,
        limit: Optional[int] = None,
        columns: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Query a table with optional filtering.
        
        Args:
            table_name: Table name
            schema_name: Schema name (defaults to self.schema)
            catalog_name: Catalog name (defaults to self.catalog)
            where_clause: Optional WHERE clause (without WHERE keyword)
            limit: Optional row limit
            columns: Optional list of columns to select (defaults to *)
            
        Returns:
            List of dictionaries representing rows
        """
        schema = schema_name or self.schema
        catalog = catalog_name or self.catalog
        
        # Build SELECT clause
        if columns:
            select_clause = ", ".join(columns)
        else:
            select_clause = "*"
        
        # Build full table name
        full_table_name = f"{catalog}.{schema}.{table_name}"
        
        # Build query
        query = f"SELECT {select_clause} FROM {full_table_name}"
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        return self.execute_query(query)
    
    def get_table_count(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        catalog_name: Optional[str] = None,
        where_clause: Optional[str] = None
    ) -> int:
        """
        Get row count for a table.
        
        Args:
            table_name: Table name
            schema_name: Schema name
            catalog_name: Catalog name
            where_clause: Optional WHERE clause
            
        Returns:
            Row count
        """
        schema = schema_name or self.schema
        catalog = catalog_name or self.catalog
        full_table_name = f"{catalog}.{schema}.{table_name}"
        
        query = f"SELECT COUNT(*) as cnt FROM {full_table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        results = self.execute_query(query)
        return results[0]['cnt'] if results else 0
    
    def list_tables(
        self,
        schema_name: Optional[str] = None,
        catalog_name: Optional[str] = None
    ) -> List[str]:
        """
        List tables in a schema.
        
        Args:
            schema_name: Schema name (defaults to self.schema)
            catalog_name: Catalog name (defaults to self.catalog)
            
        Returns:
            List of table names
        """
        schema = schema_name or self.schema
        catalog = catalog_name or self.catalog
        
        query = f"SHOW TABLES FROM {catalog}.{schema}"
        results = self.execute_query(query)
        return [row['Table'] for row in results]
    
    def list_schemas(
        self,
        catalog_name: Optional[str] = None
    ) -> List[str]:
        """
        List schemas in a catalog.
        
        Args:
            catalog_name: Catalog name (defaults to self.catalog)
            
        Returns:
            List of schema names
        """
        catalog = catalog_name or self.catalog
        query = f"SHOW SCHEMAS FROM {catalog}"
        results = self.execute_query(query)
        return [row['Schema'] for row in results]
    
    def test_connection(self) -> bool:
        """
        Test connection to Trino.
        
        Returns:
            True if connection is successful
        """
        try:
            self.execute_query("SELECT 1")
            logger.info("Trino connection test successful")
            return True
        except Exception as e:
            logger.error(f"Trino connection test failed: {e}")
            return False
    
    def close(self) -> None:
        """Close Trino connection."""
        if self.connection and not self.connection.is_closed():
            try:
                self.connection.close()
                logger.info("Trino connection closed")
            except Exception as e:
                logger.warning(f"Error closing Trino connection: {e}")
            finally:
                self.connection = None
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
