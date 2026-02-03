"""
Trino integration module for querying Iceberg tables.

This module provides functionality for connecting to Trino and executing
SQL queries against Iceberg tables in the data lake.
"""

from src.trino_integration.trino_client import TrinoClient
from src.trino_integration.catalog_manager import CatalogManager

__all__ = ['TrinoClient', 'CatalogManager']
