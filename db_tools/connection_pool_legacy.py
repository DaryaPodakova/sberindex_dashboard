#!/usr/bin/env python3
"""
Database Connection Management
Implements connection pooling, retry logic, and secure credential management
Following architectural patterns for database operations
"""

import os
import time
import logging
from typing import Dict, Any, Optional, Callable, TypeVar
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
import psycopg2
from psycopg2 import pool
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_READ_COMMITTED

logger = logging.getLogger(__name__)

T = TypeVar('T')


@dataclass
class ConnectionConfig:
    """Database connection configuration."""
    host: str
    port: str
    database: str
    user: str
    password: str
    min_connections: int = 1
    max_connections: int = 10
    connection_timeout: int = 30
    retry_attempts: int = 3
    retry_delay: float = 1.0
    isolation_level: int = ISOLATION_LEVEL_READ_COMMITTED


@dataclass
class ConnectionHealth:
    """Connection health status."""
    healthy: bool
    latency_ms: float
    version: str
    active_connections: int
    available_connections: int
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = None


class ConnectionPool:
    """
    Enhanced database connection pool with health monitoring and retry logic.
    Implements proper connection management following best practices.
    """

    def __init__(self, config: ConnectionConfig):
        """
        Initialize connection pool with configuration.

        Args:
            config: Database connection configuration
        """
        self.config = config
        self.pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """Initialize the connection pool with error handling."""
        try:
            connection_string = self._build_connection_string()

            self.pool = psycopg2.pool.ThreadedConnectionPool(
                self.config.min_connections,
                self.config.max_connections,
                connection_string,
                connect_timeout=self.config.connection_timeout
            )

            logger.info(f"Connection pool initialized: {self.config.min_connections}-{self.config.max_connections} connections")

        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise

    def _build_connection_string(self) -> str:
        """Build PostgreSQL connection string from configuration."""
        return (
            f"host={self.config.host} "
            f"port={self.config.port} "
            f"dbname={self.config.database} "
            f"user={self.config.user} "
            f"password={self.config.password}"
        )

    @contextmanager
    def get_connection(self, isolation_level: int = None):
        """
        Get connection from pool with proper cleanup.

        Args:
            isolation_level: Transaction isolation level

        Yields:
            Database connection
        """
        connection = None
        try:
            if not self.pool:
                raise RuntimeError("Connection pool not initialized")

            connection = self.pool.getconn()
            if connection is None:
                raise RuntimeError("Failed to get connection from pool")

            # Set isolation level if specified
            if isolation_level is not None:
                connection.set_isolation_level(isolation_level)
            else:
                connection.set_isolation_level(self.config.isolation_level)

            logger.debug("Connection acquired from pool")
            yield connection

        except Exception as e:
            logger.error(f"Connection error: {e}")
            if connection:
                connection.rollback()
            raise

        finally:
            if connection and self.pool:
                self.pool.putconn(connection)
                logger.debug("Connection returned to pool")

    @contextmanager
    def get_cursor(self, isolation_level: int = None):
        """
        Get cursor with automatic connection and cleanup management.

        Args:
            isolation_level: Transaction isolation level

        Yields:
            Database cursor
        """
        with self.get_connection(isolation_level) as connection:
            cursor = connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def execute_with_retry(
        self,
        operation: Callable[[psycopg2.extensions.cursor], T],
        isolation_level: int = None,
        read_only: bool = False
    ) -> T:
        """
        Execute database operation with retry logic.

        Args:
            operation: Function that takes cursor and returns result
            isolation_level: Transaction isolation level
            read_only: If True, skip commit for read-only operations

        Returns:
            Operation result

        Raises:
            Exception: If all retry attempts fail
        """
        last_exception = None

        for attempt in range(self.config.retry_attempts):
            try:
                with self.get_cursor(isolation_level) as cursor:
                    result = operation(cursor)
                    if not read_only:
                        cursor.connection.commit()
                    return result

            except Exception as e:
                last_exception = e
                logger.warning(f"Attempt {attempt + 1}/{self.config.retry_attempts} failed: {e}")

                if attempt < self.config.retry_attempts - 1:
                    time.sleep(self.config.retry_delay * (2 ** attempt))  # Exponential backoff
                else:
                    logger.error(f"All retry attempts failed. Last error: {e}")

        raise last_exception

    def health_check(self) -> ConnectionHealth:
        """
        Perform comprehensive health check on connection pool.

        Returns:
            Connection health status
        """
        start_time = time.time()

        try:
            with self.get_cursor(ISOLATION_LEVEL_AUTOCOMMIT) as cursor:
                # Test basic connectivity
                cursor.execute("SELECT version(), current_timestamp")
                version_info = cursor.fetchone()

                # Get connection stats
                cursor.execute("""
                    SELECT
                        count(*) as active_connections,
                        current_setting('max_connections')::int as max_connections
                    FROM pg_stat_activity
                    WHERE state = 'active'
                """)
                stats = cursor.fetchone()

                latency_ms = (time.time() - start_time) * 1000
                available_connections = self.config.max_connections - (stats[0] if stats else 0)

                return ConnectionHealth(
                    healthy=True,
                    latency_ms=round(latency_ms, 2),
                    version=version_info[0] if version_info else "unknown",
                    active_connections=stats[0] if stats else 0,
                    available_connections=max(0, available_connections),
                    metadata={
                        "pool_min": self.config.min_connections,
                        "pool_max": self.config.max_connections,
                        "database": self.config.database,
                        "host": self.config.host
                    }
                )

        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            logger.error(f"Health check failed: {e}")

            return ConnectionHealth(
                healthy=False,
                latency_ms=round(latency_ms, 2),
                version="unknown",
                active_connections=0,
                available_connections=0,
                error_message=str(e),
                metadata={"error_type": type(e).__name__}
            )

    def close(self) -> None:
        """Close connection pool and cleanup resources."""
        if self.pool:
            self.pool.closeall()
            self.pool = None
            logger.info("Connection pool closed")


class DatabaseManager:
    """
    High-level database manager with connection pooling and utilities.
    Implements secure credential management and connection optimization.
    """

    def __init__(self, connection_params: Dict[str, str] = None):
        """
        Initialize database manager.

        Args:
            connection_params: Override default connection parameters
        """
        self.config = self._load_config(connection_params)
        self.pool = ConnectionPool(self.config)

    def _load_config(self, override_params: Dict[str, str] = None) -> ConnectionConfig:
        """Load database configuration from environment with overrides."""
        # SECURITY: Credentials MUST come from .env file
        # No hardcoded defaults to prevent accidental password exposure
        params = {
            'host': os.getenv('DB_HOST', 'host.docker.internal'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'platform'),
            'user': os.getenv('DB_USER', 'bot_etl_user2'),
            'password': os.getenv('DB_PASSWORD')  # No default - MUST be in .env
        }

        if override_params:
            params.update(override_params)

        return ConnectionConfig(
            host=params['host'],
            port=params['port'],
            database=params['database'],
            user=params['user'],
            password=params['password'],
            min_connections=int(os.getenv('DB_POOL_MIN', '1')),
            max_connections=int(os.getenv('DB_POOL_MAX', '10')),
            connection_timeout=int(os.getenv('DB_TIMEOUT', '30')),
            retry_attempts=int(os.getenv('DB_RETRY_ATTEMPTS', '3')),
            retry_delay=float(os.getenv('DB_RETRY_DELAY', '1.0'))
        )

    @contextmanager
    def get_connection(self, isolation_level: int = None):
        """Get database connection from pool."""
        with self.pool.get_connection(isolation_level) as connection:
            yield connection

    @contextmanager
    def get_cursor(self, isolation_level: int = None):
        """Get database cursor from pool."""
        with self.pool.get_cursor(isolation_level) as cursor:
            yield cursor

    def execute_with_retry(
        self,
        operation: Callable[[psycopg2.extensions.cursor], T],
        isolation_level: int = None,
        read_only: bool = False
    ) -> T:
        """Execute operation with retry logic."""
        return self.pool.execute_with_retry(operation, isolation_level, read_only=read_only)

    def health_check(self) -> ConnectionHealth:
        """Perform health check on database connection."""
        return self.pool.health_check()

    def test_connection(self) -> Dict[str, Any]:
        """Test database connection and return diagnostics."""
        health = self.health_check()

        return {
            "connected": health.healthy,
            "latency_ms": health.latency_ms,
            "database_version": health.version,
            "active_connections": health.active_connections,
            "available_connections": health.available_connections,
            "pool_config": {
                "min_connections": self.config.min_connections,
                "max_connections": self.config.max_connections,
                "timeout": self.config.connection_timeout
            },
            "error": health.error_message
        }

    def close(self) -> None:
        """Close database manager and cleanup resources."""
        self.pool.close()


def with_db_retry(retries: int = 3, delay: float = 1.0):
    """
    Decorator for automatic database operation retry.

    Args:
        retries: Number of retry attempts
        delay: Initial delay between retries (exponential backoff)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < retries - 1:
                        time.sleep(delay * (2 ** attempt))
                        logger.warning(f"Retry {attempt + 1}/{retries} for {func.__name__}: {e}")

            logger.error(f"All retries failed for {func.__name__}: {last_exception}")
            raise last_exception

        return wrapper
    return decorator


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_database_manager() -> DatabaseManager:
    """Get global database manager instance."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def close_database_manager() -> None:
    """Close global database manager."""
    global _db_manager
    if _db_manager:
        _db_manager.close()
        _db_manager = None
