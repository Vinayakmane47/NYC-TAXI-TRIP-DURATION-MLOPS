"""
Table registry for managing Iceberg table metadata.

This module provides a registry for tracking Iceberg table locations, schemas,
and partitions across the bronze, silver, and gold layers.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional, List, Any
from datetime import datetime

from src.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


@dataclass
class TableMetadata:
    """Metadata for an Iceberg table."""
    
    table_name: str
    schema_name: str  # bronze, silver, or gold
    table_location: str  # S3 path to table
    catalog_name: str  # Trino catalog name
    partition_spec: Dict[str, Any]  # Partition specification
    schema_definition: Optional[Dict[str, Any]] = None  # Table schema
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    version: int = 1  # Iceberg table version
    record_count: Optional[int] = None  # Approximate record count


class TableRegistry:
    """
    Registry for managing Iceberg table metadata.
    
    Tracks table locations, schemas, partitions, and lifecycle information
    for bronze, silver, and gold layer tables.
    """
    
    def __init__(self, registry_file: Optional[Path] = None):
        """
        Initialize TableRegistry.
        
        Args:
            registry_file: Optional path to persistent registry file (JSON/YAML)
        """
        self.registry_file = registry_file
        self.tables: Dict[str, TableMetadata] = {}
        self._load_registry()
    
    def _load_registry(self) -> None:
        """Load registry from file if it exists."""
        if self.registry_file and self.registry_file.exists():
            try:
                import yaml
                with open(self.registry_file, 'r') as f:
                    data = yaml.safe_load(f) or {}
                
                for key, table_data in data.get('tables', {}).items():
                    table_data['created_at'] = datetime.fromisoformat(table_data['created_at']) if table_data.get('created_at') else None
                    table_data['updated_at'] = datetime.fromisoformat(table_data['updated_at']) if table_data.get('updated_at') else None
                    self.tables[key] = TableMetadata(**table_data)
                
                logger.info(f"Loaded {len(self.tables)} tables from registry")
            except Exception as e:
                logger.warning(f"Failed to load registry from {self.registry_file}: {e}")
    
    def _save_registry(self) -> None:
        """Save registry to file if configured."""
        if not self.registry_file:
            return
        
        try:
            import yaml
            self.registry_file.parent.mkdir(parents=True, exist_ok=True)
            
            data = {
                'tables': {}
            }
            
            for key, table in self.tables.items():
                table_dict = {
                    'table_name': table.table_name,
                    'schema_name': table.schema_name,
                    'table_location': table.table_location,
                    'catalog_name': table.catalog_name,
                    'partition_spec': table.partition_spec,
                    'schema_definition': table.schema_definition,
                    'created_at': table.created_at.isoformat() if table.created_at else None,
                    'updated_at': table.updated_at.isoformat() if table.updated_at else None,
                    'version': table.version,
                    'record_count': table.record_count
                }
                data['tables'][key] = table_dict
            
            with open(self.registry_file, 'w') as f:
                yaml.dump(data, f, default_flow_style=False)
            
            logger.debug(f"Saved registry to {self.registry_file}")
        except Exception as e:
            logger.warning(f"Failed to save registry to {self.registry_file}: {e}")
    
    def _get_table_key(self, schema_name: str, table_name: str) -> str:
        """Generate registry key for a table."""
        return f"{schema_name}.{table_name}"
    
    def register_table(
        self,
        schema_name: str,
        table_name: str,
        table_location: str,
        catalog_name: str,
        partition_spec: Dict[str, Any],
        schema_definition: Optional[Dict[str, Any]] = None
    ) -> TableMetadata:
        """
        Register a new Iceberg table in the registry.
        
        Args:
            schema_name: Schema name (bronze, silver, or gold)
            table_name: Table name (e.g., 'trips')
            table_location: S3 path to table location
            catalog_name: Trino catalog name
            partition_spec: Partition specification
            schema_definition: Optional table schema definition
            
        Returns:
            TableMetadata object
        """
        key = self._get_table_key(schema_name, table_name)
        
        if key in self.tables:
            logger.warning(f"Table {key} already registered, updating metadata")
            table = self.tables[key]
            table.table_location = table_location
            table.partition_spec = partition_spec
            table.schema_definition = schema_definition
            table.updated_at = datetime.now()
            table.version += 1
        else:
            table = TableMetadata(
                table_name=table_name,
                schema_name=schema_name,
                table_location=table_location,
                catalog_name=catalog_name,
                partition_spec=partition_spec,
                schema_definition=schema_definition,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            self.tables[key] = table
            logger.info(f"Registered new table: {key}")
        
        self._save_registry()
        return table
    
    def get_table(self, schema_name: str, table_name: str) -> Optional[TableMetadata]:
        """
        Get table metadata from registry.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            TableMetadata if found, None otherwise
        """
        key = self._get_table_key(schema_name, table_name)
        return self.tables.get(key)
    
    def list_tables(self, schema_name: Optional[str] = None) -> List[TableMetadata]:
        """
        List all registered tables, optionally filtered by schema.
        
        Args:
            schema_name: Optional schema name to filter by
            
        Returns:
            List of TableMetadata objects
        """
        if schema_name:
            return [t for t in self.tables.values() if t.schema_name == schema_name]
        return list(self.tables.values())
    
    def update_table_metadata(
        self,
        schema_name: str,
        table_name: str,
        **updates
    ) -> Optional[TableMetadata]:
        """
        Update table metadata.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            **updates: Key-value pairs to update
            
        Returns:
            Updated TableMetadata or None if not found
        """
        key = self._get_table_key(schema_name, table_name)
        if key not in self.tables:
            logger.warning(f"Table {key} not found in registry")
            return None
        
        table = self.tables[key]
        for attr, value in updates.items():
            if hasattr(table, attr):
                setattr(table, attr, value)
        
        table.updated_at = datetime.now()
        table.version += 1
        self._save_registry()
        
        logger.info(f"Updated metadata for table {key}")
        return table
    
    def unregister_table(self, schema_name: str, table_name: str) -> bool:
        """
        Unregister a table from the registry.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            True if table was removed, False if not found
        """
        key = self._get_table_key(schema_name, table_name)
        if key in self.tables:
            del self.tables[key]
            self._save_registry()
            logger.info(f"Unregistered table {key}")
            return True
        return False
    
    def get_table_location(self, schema_name: str, table_name: str) -> Optional[str]:
        """
        Get table location for a registered table.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            Table location (S3 path) or None if not found
        """
        table = self.get_table(schema_name, table_name)
        return table.table_location if table else None
    
    def get_full_table_name(self, schema_name: str, table_name: str) -> str:
        """
        Get full table name for Trino queries (catalog.schema.table).
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            Full table name (e.g., 'iceberg.gold.trips')
        """
        table = self.get_table(schema_name, table_name)
        if not table:
            raise ConfigurationError(f"Table {schema_name}.{table_name} not found in registry")
        
        return f"{table.catalog_name}.{schema_name}.{table_name}"
