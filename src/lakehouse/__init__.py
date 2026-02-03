"""
Lakehouse module for Iceberg table management.

This module provides functionality for managing Iceberg tables in the data lake,
including table creation, schema management, and data conversion.
"""

from src.lakehouse.iceberg_manager import IcebergManager
from src.lakehouse.table_registry import TableRegistry

__all__ = ['IcebergManager', 'TableRegistry']
