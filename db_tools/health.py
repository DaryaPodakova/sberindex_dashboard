#!/usr/bin/env python3
"""
Database Health Checker Component
Minimal wrapper around existing DatabaseValidator.validate_full_stack()
"""

import logging
from typing import Dict, Any

from .validator import DatabaseValidator

logger = logging.getLogger("db_tools")


def check_database_health() -> Dict[str, Any]:
    """
    Check database health using existing validation infrastructure
    
    Returns:
        Dict containing health status and validation results
    """
    logger.info("Starting database health check")
    
    validator = DatabaseValidator()
    result = validator.validate_full_stack()
    
    health_status = {
        'healthy': result.ready_for_etl,
        'connection_valid': result.connection_valid,
        'schema_valid': result.schema_valid,
        'validation_results': result.schema_results,
        'config_used': {
            'host': result.config_used.host,
            'port': result.config_used.port,
            'database': result.config_used.database,
            'environment': result.config_used.environment.value
        }
    }
    
    logger.info(f"Database health check complete - Healthy: {health_status['healthy']}")
    return health_status