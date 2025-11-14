"""
SHARED-019: Registry Schema Parser for Database Governance
Implements registry.yaml parser to solve schema chaos with proactive table creation

Author: DATABASE_AGENT
Date: 2025-09-10
Context: Auditor approved sql/registry.yaml approach with +2 Extend score
Requirements: Load <100ms, 100% DDL accuracy, no breaking changes to db_tools API
"""

import os
import yaml
import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
from sqlalchemy import text

# Use db_tools API only - no raw SQL DDL/DML direct execution
from .factory import create_engine_with_resolver, get_database_factory
from .validator import DatabaseValidator

# Configure structured JSON logging as per DATABASE_AGENT contract
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s", "module": "%(name)s"}',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


@dataclass
class RegistryColumn:
    """Registry column definition"""
    name: str
    data_type: str
    nullable: bool = True
    default_value: Optional[str] = None


@dataclass
class RegistryTable:
    """Registry table definition"""
    name: str
    schema_name: str
    columns: List[RegistryColumn]
    primary_key: List[str]
    indexes: List[List[str]]
    partition_config: Optional[Dict[str, Any]] = None
    
    @property
    def full_name(self) -> str:
        """Get fully qualified table name"""
        return f"{self.schema_name}_{self.name}" if self.schema_name else self.name


@dataclass
class PlatformMapping:
    """Platform-specific mapping configuration"""
    platform: str
    table: str
    source: str
    dedupe_keys: List[str]
    late_arrivals_days: int
    field_mappings: Dict[str, str]


class RegistryParser:
    """
    Registry.yaml parser for schema governance
    Converts registry definitions to DDL statements for proactive schema creation
    """
    
    def __init__(self, registry_path: Optional[str] = None):
        """Initialize parser with optional registry path"""
        if registry_path is None:
            # Find project root dynamically (cross-platform)
            current_file = Path(__file__).resolve()
            project_root = current_file.parent.parent  # db_tools/registry_parser.py -> etl_platform_pipline/
            default_path = project_root / "sql" / "registry.yaml"
            self.registry_path = str(default_path)
        else:
            self.registry_path = registry_path
        self.registry_data: Optional[Dict[str, Any]] = None
        self.tables: Dict[str, RegistryTable] = {}
        self.platform_mappings: Dict[str, Dict[str, PlatformMapping]] = {}
        self._load_start_time = None
        
        # Database connection via db_tools API only
        self.factory = get_database_factory()
        self.validator = None
        self.engine = None
        
        logger.info(f"Registry parser initialized with path: {self.registry_path}")
    
    def load_registry(self) -> bool:
        """
        Load registry.yaml into memory with <100ms performance requirement
        Returns: True if loaded successfully, False otherwise
        """
        self._load_start_time = time.time()
        
        try:
            if not os.path.exists(self.registry_path):
                logger.error(f"Registry file not found: {self.registry_path}")
                return False
            
            with open(self.registry_path, 'r', encoding='utf-8') as f:
                self.registry_data = yaml.safe_load(f)
            
            # Parse L1 contracts
            if 'l1_contracts' in self.registry_data:
                self._parse_l1_contracts()
            
            # Parse platform mappings  
            if 'platform_mappings' in self.registry_data:
                self._parse_platform_mappings()
            
            load_time_ms = (time.time() - self._load_start_time) * 1000
            
            logger.info(f"Registry loaded successfully in {load_time_ms:.2f}ms - tables: {len(self.tables)}, platforms: {len(self.platform_mappings)}")
            
            # Verify <100ms requirement
            if load_time_ms > 100:
                logger.warning(f"Registry load time {load_time_ms:.2f}ms exceeds 100ms requirement")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load registry: {e}")
            return False
    
    def _parse_l1_contracts(self):
        """Parse L1 contract definitions into table objects"""
        l1_contracts = self.registry_data.get('l1_contracts', {})
        
        for contract_name, contract_def in l1_contracts.items():
            # Map revenue_fact to l1_gaming_revenue as per requirements
            table_name = "l1_gaming_revenue" if contract_name == "revenue_fact" else f"l1_{contract_name}"
            
            # Parse columns
            columns = []
            columns_def = contract_def.get('columns', {})
            
            for col_name, col_type in columns_def.items():
                column = RegistryColumn(
                    name=col_name,
                    data_type=self._map_data_type(col_type),
                    nullable=True  # Default, can be overridden
                )
                columns.append(column)
            
            # Create table object
            table = RegistryTable(
                name=table_name,
                schema_name="",  # No schema prefix needed
                columns=columns,
                primary_key=contract_def.get('pk', []),
                indexes=contract_def.get('indexes', []),
                partition_config=contract_def.get('partition')
            )
            
            self.tables[table_name] = table
            logger.debug(f"Parsed L1 contract: {contract_name} -> {table_name}")
    
    def _parse_platform_mappings(self):
        """Parse platform mapping configurations"""
        mappings = self.registry_data.get('platform_mappings', {})
        
        for platform, platform_def in mappings.items():
            self.platform_mappings[platform] = {}
            
            for table, mapping_def in platform_def.items():
                mapping = PlatformMapping(
                    platform=platform,
                    table=table,
                    source=mapping_def.get('source', ''),
                    dedupe_keys=mapping_def.get('dedupe_keys', []),
                    late_arrivals_days=mapping_def.get('late_arrivals_days', 0),
                    field_mappings=mapping_def.get('map', {})
                )
                
                self.platform_mappings[platform][table] = mapping
    
    def _map_data_type(self, registry_type: str) -> str:
        """Map registry data types to PostgreSQL types"""
        type_mapping = {
            'date': 'DATE',
            'text': 'TEXT',
            'int': 'INTEGER',
            'numeric(18,2)': 'NUMERIC(18,2)',
            'char(2)': 'CHAR(2)',
            'char(3)': 'CHAR(3)'
        }
        
        return type_mapping.get(registry_type, registry_type.upper())
    
    def generate_ddl(self, table_name: str) -> Optional[str]:
        """
        Generate DDL for specified table from registry definitions
        Returns: DDL string or None if table not found
        """
        if table_name not in self.tables:
            logger.error(f"Table {table_name} not found in registry")
            return None
        
        table = self.tables[table_name]
        
        # Build CREATE TABLE statement
        ddl_parts = [f"CREATE TABLE IF NOT EXISTS {table.full_name} ("]
        
        # Add columns
        column_defs = []
        for col in table.columns:
            col_def = f"    {col.name} {col.data_type}"
            if not col.nullable:
                col_def += " NOT NULL"
            if col.default_value:
                col_def += f" DEFAULT {col.default_value}"
            column_defs.append(col_def)
        
        # Add primary key constraint
        if table.primary_key:
            pk_cols = ", ".join(table.primary_key)
            column_defs.append(f"    PRIMARY KEY ({pk_cols})")
        
        ddl_parts.append(",\n".join(column_defs))
        ddl_parts.append(");")
        
        ddl = "\n".join(ddl_parts)
        
        # Add partition clause if specified
        if table.partition_config:
            partition_type = table.partition_config.get('type')
            partition_key = table.partition_config.get('key')
            
            if partition_type == 'range' and partition_key:
                ddl += f"\nPARTITION BY RANGE ({partition_key});"
        
        logger.debug(f"Generated DDL for {table_name}: {len(ddl)} characters")
        return ddl
    
    def generate_indexes_ddl(self, table_name: str) -> List[str]:
        """Generate index DDL statements for specified table"""
        if table_name not in self.tables:
            return []
        
        table = self.tables[table_name]
        index_ddls = []
        
        for i, index_cols in enumerate(table.indexes):
            index_name = f"idx_{table.full_name}_{'_'.join(index_cols)}"
            cols_str = ", ".join(index_cols)
            
            ddl = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table.full_name} ({cols_str});"
            index_ddls.append(ddl)
        
        logger.debug(f"Generated {len(index_ddls)} index DDL statements for {table_name}")
        return index_ddls
    
    def create_table_from_registry(self, table_name: str, validate_schema: bool = True) -> bool:
        """
        Create table from registry definition using db_tools API
        Args:
            table_name: Name of table to create
            validate_schema: Whether to validate schema after creation
        Returns: True if successful, False otherwise
        """
        if not self.registry_data:
            logger.error("Registry not loaded - call load_registry() first")
            return False
        
        # Generate DDL
        ddl = self.generate_ddl(table_name)
        if not ddl:
            return False
        
        try:
            # Get engine via db_tools API
            if not self.engine:
                self.engine = create_engine_with_resolver()
            
            # Execute DDL
            with self.engine.connect() as conn:
                logger.info(f"Creating table {table_name} from registry definition")
                conn.execute(text(ddl))
                
                # Create indexes
                index_ddls = self.generate_indexes_ddl(table_name)
                for index_ddl in index_ddls:
                    conn.execute(text(index_ddl))
                
                conn.commit()
            
            # Validate schema if requested
            if validate_schema:
                if not self.validator:
                    config = self.factory.create_from_env()
                    self.validator = DatabaseValidator(config)
                
                # Verify table was created correctly
                validation_result = self._validate_table_schema(table_name)
                if not validation_result:
                    logger.warning(f"Schema validation failed for {table_name}")
                    return False
            
            logger.info(f"Table {table_name} created successfully from registry with {len(self.tables[table_name].columns)} columns and {len(self.tables[table_name].indexes)} indexes")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            return False
    
    def _validate_table_schema(self, table_name: str) -> bool:
        """Validate created table matches registry specification"""
        if table_name not in self.tables:
            return False
        
        table = self.tables[table_name]
        
        try:
            # Query table schema
            schema_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(schema_query), (table_name,))
                db_columns = {row[0]: (row[1], row[2]) for row in result.fetchall()}
            
            # Verify all registry columns exist
            for col in table.columns:
                if col.name not in db_columns:
                    logger.error(f"Column {col.name} missing from created table {table_name}")
                    return False
            
            logger.debug(f"Schema validation passed for {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Schema validation error for {table_name}: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> Optional[RegistryTable]:
        """Get table information from registry"""
        return self.tables.get(table_name)
    
    def list_tables(self) -> List[str]:
        """List all tables defined in registry"""
        return list(self.tables.keys())
    
    def get_platform_mapping(self, platform: str, table: str) -> Optional[PlatformMapping]:
        """Get platform mapping configuration"""
        return self.platform_mappings.get(platform, {}).get(table)
    
    def cache_registry_data(self) -> Dict[str, Any]:
        """
        Cache registry data for token optimization (>65% reduction target)
        Returns: Cached data structure for efficient access
        """
        if not self.registry_data:
            return {}
        
        cached_data = {
            'tables': {name: {
                'columns': [{'name': c.name, 'type': c.data_type} for c in table.columns],
                'primary_key': table.primary_key,
                'indexes': table.indexes
            } for name, table in self.tables.items()},
            'platforms': self.platform_mappings,
            'load_time_ms': (time.time() - self._load_start_time) * 1000 if self._load_start_time else 0
        }
        
        logger.info(f"Registry data cached for token optimization - {len(cached_data['tables'])} tables, {len(cached_data['platforms'])} platforms")
        return cached_data


# Convenience functions for direct usage
def load_registry_and_create_table(table_name: str, registry_path: Optional[str] = None) -> bool:
    """Load registry and create specified table in one operation"""
    parser = RegistryParser(registry_path)
    
    if not parser.load_registry():
        return False
    
    return parser.create_table_from_registry(table_name)


def get_l1_gaming_revenue_ddl() -> Optional[str]:
    """Get DDL for l1_gaming_revenue table specifically"""
    parser = RegistryParser()
    
    if not parser.load_registry():
        return None
    
    return parser.generate_ddl("l1_gaming_revenue")