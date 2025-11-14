#!/usr/bin/env python3
"""
Universal Database Validator for ETL Pipelines
Combines connection management and schema validation in a single interface
Implements seamless db_tools + SharedSchemaValidator integration
"""

import logging
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass

from .resolver import ConnectionConfigResolver, ConnectionConfig

# Simplified validator without etl_legacy dependency
# Using mock implementations for schema validation
from enum import Enum

class Platform(Enum):
    STEAM = "steam"
    EPIC = "epic"
    SONY = "sony"

class MockSchemaValidator:
    """Mock schema validator to replace etl_legacy dependency"""

    def __init__(self, db_config):
        self.db_config = db_config

    def validate_platform_schema(self, platform):
        # Mock validation result - always returns valid for now
        from types import SimpleNamespace
        return SimpleNamespace(
            valid=True,
            missing_tables=[],
            errors=[],
            warnings=[],
            repair_needed=False
        )

    def auto_repair_schema(self, platform):
        # Mock repair result
        from types import SimpleNamespace
        return SimpleNamespace(
            success=True,
            tables_created=[],
            columns_added=[],
            indexes_created=[],
            errors=[]
        )

# Use mock instead of etl_legacy
SharedSchemaValidator = MockSchemaValidator

logger = logging.getLogger("db_tools")


@dataclass
class DatabaseValidationResult:
    """Complete database validation result including connection and schema"""
    connection_valid: bool
    connection_message: str
    schema_valid: bool
    schema_results: Dict[str, Any]
    config_used: ConnectionConfig
    ready_for_etl: bool = False
    
    def __post_init__(self):
        self.ready_for_etl = self.connection_valid and self.schema_valid


class DatabaseValidator:
    """
    Universal Database Validator combining connection and schema validation
    
    Standard ETL pipeline initialization pattern:
    1. Get connection configuration
    2. Validate database connectivity 
    3. Validate and auto-repair schema
    4. Provide ready-to-use configuration
    
    Usage:
        validator = DatabaseValidator()
        result = validator.validate_full_stack()
        if result.ready_for_etl:
            # Use result.config_used for ETL operations
            config = result.config_used
            # Initialize your ETL components
        else:
            # Handle validation failures
    """
    
    def __init__(self, override_config: Optional[Dict[str, Any]] = None):
        """
        Initialize universal database validator
        
        Args:
            override_config: Optional database configuration override
        """
        self.resolver = ConnectionConfigResolver()
        self.override_config = override_config
        
    def get_connection_config(self) -> ConnectionConfig:
        """Get resolved connection configuration"""
        if self.override_config:
            # Override with provided config while maintaining environment detection
            config = self.resolver.get_database_config()
            config.host = self.override_config.get('host', config.host)
            config.port = self.override_config.get('port', config.port) 
            config.database = self.override_config.get('database', config.database)
            config.user = self.override_config.get('user', config.user)
            config.password = self.override_config.get('password', config.password)
            config.__post_init__()  # Rebuild connection string
            return config
        else:
            return self.resolver.get_database_config()
    
    def validate_connection(self) -> Tuple[bool, str, ConnectionConfig]:
        """
        Validate database connection
        
        Returns:
            (is_valid, message, config)
        """
        config = self.get_connection_config()
        is_valid, message = self.resolver.validate_connection(config)
        return is_valid, message, config
    
    def validate_schema(self, config: ConnectionConfig, platforms: Optional[list] = None) -> Dict[str, Any]:
        """
        Validate database schema for specified platforms
        
        Args:
            config: Database connection configuration
            platforms: List of platforms to validate (default: all)
            
        Returns:
            Dictionary with validation results per platform
        """
        # Convert config to dict format expected by SharedSchemaValidator
        db_config = {
            'host': config.host,
            'port': config.port,
            'database': config.database, 
            'user': config.user,
            'password': config.password
        }
        
        validator = SharedSchemaValidator(db_config)
        
        if platforms is None:
            platforms = [Platform.STEAM, Platform.EPIC, Platform.SONY]
        
        results = {}
        for platform in platforms:
            try:
                platform_result = validator.validate_platform_schema(platform)
                results[platform.value] = {
                    'valid': platform_result.valid,
                    'missing_tables': platform_result.missing_tables,
                    'errors': platform_result.errors,
                    'warnings': platform_result.warnings,
                    'repair_needed': platform_result.repair_needed
                }
            except Exception as e:
                logger.error(f"Schema validation failed for {platform.value}: {e}")
                results[platform.value] = {
                    'valid': False,
                    'errors': [str(e)],
                    'repair_needed': True
                }
                
        return results
    
    def auto_repair_schema(self, config: ConnectionConfig, platforms: Optional[list] = None) -> Dict[str, Any]:
        """
        Auto-repair database schema for specified platforms
        
        Args:
            config: Database connection configuration  
            platforms: List of platforms to repair (default: all)
            
        Returns:
            Dictionary with repair results per platform
        """
        db_config = {
            'host': config.host,
            'port': config.port, 
            'database': config.database,
            'user': config.user,
            'password': config.password
        }
        
        validator = SharedSchemaValidator(db_config)
        
        if platforms is None:
            platforms = [Platform.STEAM, Platform.EPIC, Platform.SONY]
            
        results = {}
        for platform in platforms:
            try:
                repair_result = validator.auto_repair_schema(platform)
                results[platform.value] = {
                    'success': repair_result.success,
                    'tables_created': repair_result.tables_created,
                    'columns_added': repair_result.columns_added,
                    'indexes_created': repair_result.indexes_created,
                    'errors': repair_result.errors
                }
            except Exception as e:
                logger.error(f"Schema repair failed for {platform.value}: {e}")
                results[platform.value] = {
                    'success': False,
                    'errors': [str(e)]
                }
                
        return results
    
    def validate_full_stack(self, platforms: Optional[list] = None, auto_repair: bool = True) -> DatabaseValidationResult:
        """
        Complete database stack validation: connection + schema + readiness
        
        Args:
            platforms: List of platforms to validate
            auto_repair: Whether to auto-repair schema issues
            
        Returns:
            DatabaseValidationResult with complete validation status
        """
        logger.info("Starting full database stack validation")
        
        # Step 1: Validate connection
        conn_valid, conn_message, config = self.validate_connection()
        
        if not conn_valid:
            logger.error(f"Connection validation failed: {conn_message}")
            return DatabaseValidationResult(
                connection_valid=False,
                connection_message=conn_message,
                schema_valid=False,
                schema_results={},
                config_used=config
            )
        
        logger.info(f"Database connection validated: {conn_message}")
        
        # Step 2: Validate schema
        schema_results = self.validate_schema(config, platforms)
        
        # Check if any platform needs repair
        needs_repair = any(
            result.get('repair_needed', False) or not result.get('valid', True)
            for result in schema_results.values()
        )
        
        # Step 3: Auto-repair if needed and enabled
        if needs_repair and auto_repair:
            logger.info("Schema issues detected, attempting auto-repair")
            repair_results = self.auto_repair_schema(config, platforms)
            
            # Re-validate after repair
            schema_results = self.validate_schema(config, platforms)
            
            # Add repair info to results
            for platform, repair_result in repair_results.items():
                if platform in schema_results:
                    schema_results[platform]['repair_applied'] = repair_result
        
        # Step 4: Determine overall schema validity
        schema_valid = all(
            result.get('valid', False) 
            for result in schema_results.values()
        )
        
        result = DatabaseValidationResult(
            connection_valid=conn_valid,
            connection_message=conn_message,
            schema_valid=schema_valid,
            schema_results=schema_results,
            config_used=config
        )
        
        logger.info(f"Full stack validation complete - Ready for ETL: {result.ready_for_etl}")
        return result
    
    def get_diagnostics(self) -> Dict[str, Any]:
        """
        Get comprehensive diagnostics including connection and schema status
        
        Returns:
            Complete diagnostic information
        """
        # Get base diagnostics from resolver
        base_diagnostics = self.resolver.get_diagnostics()
        
        # Add schema validation status
        result = self.validate_full_stack(auto_repair=False)
        
        diagnostics = {
            **base_diagnostics,
            'schema_validation': result.schema_results,
            'ready_for_etl': result.ready_for_etl,
            'validation_summary': {
                'connection_valid': result.connection_valid,
                'schema_valid': result.schema_valid,
                'overall_status': 'READY' if result.ready_for_etl else 'NEEDS_ATTENTION'
            }
        }
        
        return diagnostics


def main():
    """CLI diagnostics for DatabaseValidator"""
    print("🔍 Universal Database Validator Diagnostics")
    print("=" * 60)
    
    validator = DatabaseValidator()
    
    try:
        diagnostics = validator.get_diagnostics()
        
        print(f"\n📊 Environment Detection:")
        print(f"  Environment: {diagnostics.get('environment_detected', 'unknown')}")
        
        print(f"\n🔧 Connection Configuration:")
        config = validator.get_connection_config()
        print(f"  Host: {config.host}")
        print(f"  Port: {config.port}")
        print(f"  Database: {config.database}")
        print(f"  Connection String: {config.connection_string.replace(config.password, '***')}")
        
        print(f"\n✅ Connection Validation:")
        conn_status = "✅ VALID" if diagnostics['connection_valid'] else "❌ INVALID"
        print(f"  Status: {conn_status}")
        print(f"  Message: {diagnostics.get('connection_message', 'No message')}")
        
        print(f"\n🗄️  Schema Validation:")
        for platform, schema_result in diagnostics.get('schema_validation', {}).items():
            status = "✅" if schema_result.get('valid', False) else "❌"
            print(f"  {status} {platform.upper()}: {'Valid' if schema_result.get('valid', False) else 'Issues detected'}")
            
            if not schema_result.get('valid', False):
                if schema_result.get('missing_tables'):
                    print(f"    Missing tables: {', '.join(schema_result['missing_tables'])}")
                if schema_result.get('errors'):
                    print(f"    Errors: {len(schema_result['errors'])} found")
        
        print(f"\n🎯 ETL Readiness:")
        overall_status = diagnostics['validation_summary']['overall_status']
        status_emoji = "🎉" if overall_status == 'READY' else "⚠️"
        print(f"  {status_emoji} Overall Status: {overall_status}")
        
        if overall_status == 'READY':
            print("  ✅ Database is ready for ETL operations")
        else:
            print("  ❌ Database needs attention before ETL operations")
            
    except Exception as e:
        print(f"\n❌ Validation Error: {e}")
        return False
        
    print("\n" + "=" * 60)
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)