#!/usr/bin/env python3
"""
Database Tools Migration Utilities
Helper functions to migrate ETL scripts from hardcoded connections to db_tools.resolver
"""

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from .resolver import ConnectionConfigResolver
from .validator import DatabaseValidator


def create_engine_with_resolver(validator: DatabaseValidator = None, validate_schema: bool = True) -> Engine:
    """
    Create SQLAlchemy engine using db_tools.resolver with optional schema validation
    
    Args:
        validator: Optional DatabaseValidator instance to reuse
        validate_schema: Whether to validate schema before returning engine
        
    Returns:
        SQLAlchemy Engine ready for use
        
    Raises:
        RuntimeError: If connection or schema validation fails
    """
    if validator is None:
        validator = DatabaseValidator()
    
    if validate_schema:
        result = validator.validate_full_stack(auto_repair=True)
        if not result.ready_for_etl:
            error_msg = f"Database validation failed: {result.connection_message}"
            if not result.schema_valid:
                schema_errors = []
                for platform, schema_result in result.schema_results.items():
                    if not schema_result.get('valid', False):
                        schema_errors.extend(schema_result.get('errors', []))
                error_msg += f" Schema errors: {'; '.join(schema_errors)}"
            raise RuntimeError(error_msg)
        
        config = result.config_used
    else:
        is_valid, message, config = validator.validate_connection()
        if not is_valid:
            raise RuntimeError(f"Database connection failed: {message}")
    
    engine = create_engine(config.connection_string)
    return engine


def get_legacy_replacement_pattern() -> str:
    """
    Get the standard replacement pattern for legacy connection code
    
    Returns:
        String showing the recommended migration pattern
    """
    return """
# OLD PATTERN (don't use):
connection_string = "postgresql://user:pass@localhost:5432/db"
engine = create_engine(connection_string)

# NEW PATTERN (use this):
from db_tools.migration_utils import create_engine_with_resolver
engine = create_engine_with_resolver()

# OR with custom validation:
from db_tools import DatabaseValidator
validator = DatabaseValidator()
engine = create_engine_with_resolver(validator, validate_schema=True)
"""


def migrate_connection_config_dict_to_resolver() -> dict:
    """
    Get connection configuration as dictionary (compatible with legacy code)
    
    Returns:
        Dictionary with connection parameters
    """
    resolver = ConnectionConfigResolver()
    config = resolver.get_database_config()
    
    return {
        'host': config.host,
        'port': config.port,
        'database': config.database, 
        'user': config.user,
        'password': config.password,
        'connection_string': config.connection_string
    }


def migrate_psycopg2_connection():
    """
    Get psycopg2 connection using resolver (for direct psycopg2 usage)
    
    Returns:
        psycopg2 connection object
    """
    import psycopg2
    resolver = ConnectionConfigResolver()
    config = resolver.get_database_config()
    
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.user, 
        password=config.password
    )


def validate_migration_success(engine: Engine) -> bool:
    """
    Validate that migrated connection works correctly
    
    Args:
        engine: SQLAlchemy engine to test
        
    Returns:
        True if migration successful, False otherwise
    """
    try:
        with engine.connect() as conn:
            result = conn.execute("SELECT version()")
            version_info = result.fetchone()[0]
            if "PostgreSQL" in version_info:
                return True
    except Exception:
        pass
    return False


def get_migration_checklist() -> list:
    """
    Get checklist for ETL script migration
    
    Returns:
        List of migration steps
    """
    return [
        "1. Replace hardcoded connection strings with db_tools.resolver",
        "2. Import create_engine_with_resolver instead of direct create_engine", 
        "3. Remove hardcoded host/port/credentials from code",
        "4. Test connection using validate_migration_success()",
        "5. Update any psycopg2 direct connections to use migrate_psycopg2_connection()",
        "6. Remove old connection test files and imports",
        "7. Verify environment variables are set correctly",
        "8. Test in both Docker and localhost environments"
    ]


# QA_TESTING_REQUIREMENTS:
# - Unit Tests: create_engine_with_resolver(), migrate_connection_config_dict_to_resolver()
# - Integration Tests: Full ETL pipeline with migrated connections 
# - Mock Strategy: Mock DatabaseValidator, ConnectionConfigResolver for testing
# - Test Data: Valid/invalid connection configs, schema validation scenarios
# - Edge Cases: Connection failures, schema repair failures, missing env vars
# - Performance: Connection establishment timing with validation enabled/disabled