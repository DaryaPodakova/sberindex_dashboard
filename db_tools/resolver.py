#!/usr/bin/env python3
"""
Unified Connection Configuration Resolver
Implements auditor-agent recommendations: environment-aware database connections
Solves ST-008 and database connectivity issues across all ETL pipelines
"""

import os
import sys
import socket
import logging
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

logger = logging.getLogger("db_tools")

class EnvironmentType(Enum):
    """Detected environment types"""
    DOCKER_COMPOSE = "docker_compose"
    DOCKER_HOST = "docker_host" 
    LOCAL_HOST = "local_host"
    CI_CD = "ci_cd"
    UNKNOWN = "unknown"

@dataclass
class ConnectionConfig:
    """Database connection configuration"""
    host: str
    port: int
    database: str
    user: str
    password: str
    environment: EnvironmentType
    connection_string: str = ""
    
    def __post_init__(self):
        self.connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

class ConnectionConfigResolver:
    """
    Unified database connection configuration resolver
    Implements environment detection and automatic configuration selection
    """
    
    def __init__(self):
        self.environment = self._detect_environment()
        self._load_env_file()
        logger.info(f"Detected environment: {self.environment.value}")
    
    def _detect_environment(self) -> EnvironmentType:
        """Detect current runtime environment"""

        # Check for disabled Docker detection flag
        if os.environ.get("DISABLE_DOCKER_DETECT") == "1":
            logger.info("Docker detection disabled via DISABLE_DOCKER_DETECT=1")
            # Return LOCAL_HOST when docker detection is disabled
            return EnvironmentType.LOCAL_HOST

        # Check for CI/CD environment
        if any(env in os.environ for env in ['CI', 'GITHUB_ACTIONS', 'JENKINS_URL']):
            return EnvironmentType.CI_CD

        # Check for Docker environment
        if os.path.exists('/.dockerenv'):
            return EnvironmentType.DOCKER_HOST

        # Check for Docker Compose (look for service discovery)
        if os.environ.get('COMPOSE_PROJECT_NAME') or self._check_docker_compose_services():
            return EnvironmentType.DOCKER_COMPOSE

        # Default to local host
        return EnvironmentType.LOCAL_HOST
    
    def _check_docker_compose_services(self) -> bool:
        """Check if running in Docker Compose environment"""

        # Check for disabled Docker detection flag (early return)
        if os.environ.get("DISABLE_DOCKER_DETECT") == "1":
            logger.debug("Skipping Docker Compose detection due to DISABLE_DOCKER_DETECT=1")
            return False

        try:
            # Try to resolve typical compose service names
            compose_hosts = ['postgres', 'db', 'database', 'postgresql']
            for host in compose_hosts:
                try:
                    socket.gethostbyname(host)
                    logger.debug(f"Found Docker Compose service: {host}")
                    return True
                except socket.gaierror:
                    continue
        except Exception as e:
            logger.debug(f"Docker Compose detection error: {e}")
        return False
    
    def _load_env_file(self) -> None:
        """
        Load environment variables from .env file using multiple methods
        Provides POSIX shell compatibility for Docker environments
        """
        # Try to find project root dynamically
        current_file = Path(__file__).resolve()
        project_root = current_file.parent.parent  # db_tools/resolver.py -> etl_platform_pipline/

        env_paths = [
            Path("etl/.env"),  # Relative to current working dir
            project_root / ".env",  # Project root (cross-platform)
            Path(".env"),  # Current directory
        ]
        
        for env_path in env_paths:
            if env_path.exists():
                try:
                    # Method 1: Try python-dotenv if available
                    try:
                        from dotenv import load_dotenv
                        load_dotenv(env_path)
                        logger.info(f"Environment loaded using python-dotenv from {env_path}")
                        return
                    except ImportError:
                        pass
                    
                    # Method 2: Manual parsing for POSIX shell compatibility
                    self._parse_env_file(env_path)
                    logger.info(f"Environment loaded manually from {env_path}")
                    return
                    
                except Exception as e:
                    logger.warning(f"Failed to load env file {env_path}: {e}")
                    continue
        
        logger.warning("No .env file found or could be loaded")
    
    def _parse_env_file(self, env_path: Path) -> None:
        """
        Manually parse .env file and set environment variables
        POSIX shell compatible alternative to 'source .env'
        """
        try:
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    
                    # Skip comments and empty lines
                    if not line or line.startswith('#'):
                        continue
                    
                    # Skip lines with variable substitution (they need shell processing)
                    if '${' in line or '$(' in line:
                        logger.debug(f"Skipping line with variable substitution: {line}")
                        continue
                    
                    # Parse KEY=VALUE pairs
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip().strip('"').strip("'")
                        
                        # Only set if not already in environment (preserve existing values)
                        if key not in os.environ:
                            os.environ[key] = value
                            logger.debug(f"Set environment variable: {key}")
                            
        except Exception as e:
            logger.error(f"Error parsing env file {env_path}: {e}")
    
    def _test_connection_host(self, host: str, port: int, timeout: float = 2.0) -> bool:
        """Test if host:port is accessible"""
        try:
            sock = socket.create_connection((host, port), timeout=timeout)
            sock.close()
            logger.debug(f"Connection test passed: {host}:{port}")
            return True
        except (socket.error, socket.timeout) as e:
            logger.debug(f"Connection test failed for {host}:{port}: {e}")
            return False
    
    def get_database_config(self) -> ConnectionConfig:
        """
        Get database configuration based on environment detection
        Implements priority-based connection resolution
        """
        
        # Priority 1: Environment variables (highest priority)
        env_config = self._get_env_config()
        if env_config:
            logger.info(f"Using environment variable configuration: {env_config.host}:{env_config.port}")
            return env_config
        
        # Priority 2: Environment-specific defaults (only if env vars missing)
        env_defaults = self._get_environment_defaults()
        for config in env_defaults:
            if self._test_connection_host(config.host, config.port):
                logger.info(f"Using environment default configuration: {config.host}:{config.port}")
                return config
        
        # Priority 3: Fallback configuration (may not be accessible)
        fallback_config = self._get_fallback_config()
        logger.warning(f"Using fallback configuration (may fail): {fallback_config.host}:{fallback_config.port}")
        return fallback_config
    
    def _get_env_config(self) -> Optional[ConnectionConfig]:
        """Get configuration from environment variables"""
        host = os.environ.get('DB_HOST') or os.environ.get('POSTGRES_HOST')
        port = os.environ.get('DB_PORT') or os.environ.get('POSTGRES_PORT')
        database = os.environ.get('DB_NAME') or os.environ.get('POSTGRES_DB')
        user = os.environ.get('DB_USER') or os.environ.get('POSTGRES_USER')
        password = os.environ.get('DB_PASSWORD') or os.environ.get('POSTGRES_PASSWORD')
        
        if host and port and database and user and password:
            return ConnectionConfig(
                host=host,
                port=int(port),
                database=database,
                user=user,
                password=password,
                environment=self.environment
            )
        return None
    
    def _get_environment_defaults(self) -> list[ConnectionConfig]:
        """Get environment-specific default configurations in priority order"""
        
        configs = []
        
        if self.environment == EnvironmentType.DOCKER_COMPOSE:
            # Docker Compose service discovery
            compose_hosts = ['postgres', 'db', 'database', 'postgresql']
            for host in compose_hosts:
                configs.append(ConnectionConfig(
                    host=host,
                    port=5432,
                    database='platform',
                    user='postgres',
                    password='postgres',
                    environment=self.environment
                ))
        
        elif self.environment == EnvironmentType.DOCKER_HOST:
            # Docker container trying to reach external host
            docker_hosts = ['host.docker.internal', 'host-gateway', 'gateway.docker.internal']
            for host in docker_hosts:
                configs.append(ConnectionConfig(
                    host=host,
                    port=5432,
                    database='platform',
                    user='postgres', 
                    password='postgres',
                    environment=self.environment
                ))
        
        elif self.environment == EnvironmentType.LOCAL_HOST:
            # Local development environment
            local_hosts = ['localhost', '127.0.0.1']
            for host in local_hosts:
                configs.append(ConnectionConfig(
                    host=host,
                    port=5432,
                    database='platform',
                    user='postgres',
                    password='postgres',
                    environment=self.environment
                ))
        
        elif self.environment == EnvironmentType.CI_CD:
            # CI/CD environment (usually localhost services)
            configs.append(ConnectionConfig(
                host='localhost',
                port=5432,
                database='platform', 
                user='postgres',
                password='postgres',
                environment=self.environment
            ))
        
        return configs
    
    def _get_fallback_config(self) -> ConnectionConfig:
        """Get fallback configuration when all else fails"""
        return ConnectionConfig(
            host='localhost',
            port=5432,
            database='platform',
            user='postgres',
            password='postgres',
            environment=EnvironmentType.UNKNOWN
        )
    
    def validate_connection(self, config: ConnectionConfig) -> Tuple[bool, str]:
        """
        Validate database connection configuration
        Returns (success, error_message)
        """
        try:
            # Test network connectivity
            if not self._test_connection_host(config.host, config.port, timeout=5.0):
                return False, f"Cannot connect to {config.host}:{config.port} - PostgreSQL service not available"
            
            # Test database connectivity (requires psycopg2)
            try:
                import psycopg2
                conn = psycopg2.connect(
                    host=config.host,
                    port=config.port,
                    database=config.database,
                    user=config.user,
                    password=config.password,
                    connect_timeout=5
                )
                cursor = conn.cursor()
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
                cursor.close()
                conn.close()
                
                logger.info(f"PostgreSQL connection successful: {version}")
                return True, f"Connected to PostgreSQL: {version}"
                
            except ImportError:
                logger.warning("psycopg2 not available - only network connectivity tested")
                return True, "Network connectivity confirmed (psycopg2 not available for full test)"
            except psycopg2.Error as e:
                return False, f"PostgreSQL connection failed: {e}"
                
        except Exception as e:
            return False, f"Connection validation error: {e}"
    
    def get_diagnostics(self) -> Dict[str, Any]:
        """Get comprehensive connection diagnostics"""
        config = self.get_database_config()
        is_valid, validation_message = self.validate_connection(config)
        
        return {
            'environment_detected': self.environment.value,
            'selected_config': {
                'host': config.host,
                'port': config.port,
                'database': config.database,
                'connection_string': config.connection_string.replace(config.password, '***')
            },
            'connection_valid': is_valid,
            'validation_message': validation_message,
            'environment_variables': {
                'DB_HOST': os.environ.get('DB_HOST', 'Not set'),
                'POSTGRES_HOST': os.environ.get('POSTGRES_HOST', 'Not set'),
                'DB_PORT': os.environ.get('DB_PORT', 'Not set'),
                'COMPOSE_PROJECT_NAME': os.environ.get('COMPOSE_PROJECT_NAME', 'Not set')
            },
            'network_tests': self._run_network_diagnostics(config)
        }
    
    def _run_network_diagnostics(self, config: ConnectionConfig) -> Dict[str, bool]:
        """Run network connectivity diagnostics"""
        tests = {}
        
        # Test different host variants
        test_hosts = [
            ('localhost', 5432),
            ('127.0.0.1', 5432), 
            ('host.docker.internal', 5432),
            (config.host, config.port)
        ]
        
        for host, port in test_hosts:
            tests[f"{host}:{port}"] = self._test_connection_host(host, port)
        
        return tests


def main():
    """Test the connection configuration resolver"""
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    print("🔍 Connection Configuration Resolver Diagnostics")
    print("=" * 60)
    
    resolver = ConnectionConfigResolver()
    diagnostics = resolver.get_diagnostics()
    
    print(f"\n📊 Environment Detection:")
    print(f"  Environment: {diagnostics['environment_detected']}")
    
    print(f"\n🔧 Selected Configuration:")
    config_info = diagnostics['selected_config']
    print(f"  Host: {config_info['host']}")
    print(f"  Port: {config_info['port']}")
    print(f"  Database: {config_info['database']}")
    print(f"  Connection String: {config_info['connection_string']}")
    
    print(f"\n✅ Connection Validation:")
    status = "✅ VALID" if diagnostics['connection_valid'] else "❌ FAILED"
    print(f"  Status: {status}")
    print(f"  Message: {diagnostics['validation_message']}")
    
    print(f"\n🌐 Network Tests:")
    for endpoint, result in diagnostics['network_tests'].items():
        status = "✅" if result else "❌"
        print(f"  {status} {endpoint}")
    
    print(f"\n🔑 Environment Variables:")
    for var, value in diagnostics['environment_variables'].items():
        print(f"  {var}: {value}")
    
    # Summary
    print(f"\n{'=' * 60}")
    if diagnostics['connection_valid']:
        print("🎉 DATABASE CONNECTION: SUCCESS")
        print("   ✅ PostgreSQL is accessible and ready for ETL operations")
        return True
    else:
        print("❌ DATABASE CONNECTION: FAILED")
        print("   🔧 PostgreSQL service needs to be started or configured")
        print(f"   💡 Error: {diagnostics['validation_message']}")
        
        # Provide specific recommendations
        env = diagnostics['environment_detected']
        if env == 'local_host':
            print("\n💡 Recommendations for local environment:")
            print("   1. Install PostgreSQL: apt-get install postgresql postgresql-contrib")
            print("   2. Start service: sudo systemctl start postgresql")
            print("   3. Create database: createdb platform")
        elif env == 'docker_host':
            print("\n💡 Recommendations for Docker environment:")
            print("   1. Start external PostgreSQL service")
            print("   2. Use docker-compose with PostgreSQL service")
            print("   3. Check Docker network configuration")
        
        return False

# Convenience function for backward compatibility with Epic orchestrator
def resolve_database_config() -> ConnectionConfig:
    """
    Convenience function that creates a resolver and returns database config
    Provides backward compatibility for Epic orchestrator imports
    """
    resolver = ConnectionConfigResolver()
    return resolver.get_database_config()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)