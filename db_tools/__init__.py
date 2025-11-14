"""
Database Tools Module
Centralized database connectivity and diagnostics for AG Platform Pipeline
Environment-aware database connections, schema validation, and network scanning
"""

from .resolver import ConnectionConfigResolver, ConnectionConfig, EnvironmentType
from .scanner import scan_postgres_ports
from .validator import DatabaseValidator, DatabaseValidationResult
from .migration_utils import create_engine_with_resolver, migrate_connection_config_dict_to_resolver
from .health import check_database_health
from .schema_manager import DDLManager, DDLExecutionResult, SchemaValidationResult
from .factory import (
    DatabaseConfigFactory,
    DatabaseConnectionInfo,
    get_database_factory,
    create_config_from_env,
    create_engine_with_resolver as factory_create_engine,
    create_validated_config
)

__version__ = "1.0.0"
__all__ = [
    "ConnectionConfigResolver",
    "ConnectionConfig",
    "EnvironmentType",
    "scan_postgres_ports",
    "DatabaseValidator",
    "DatabaseValidationResult",
    "create_engine_with_resolver",
    "migrate_connection_config_dict_to_resolver",
    "check_database_health",
    "DDLManager",
    "DDLExecutionResult",
    "SchemaValidationResult",
    "DatabaseConfigFactory",
    "DatabaseConnectionInfo",
    "get_database_factory",
    "create_config_from_env",
    "factory_create_engine",
    "create_validated_config"
]