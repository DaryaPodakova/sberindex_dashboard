"""
Database Configuration Factory
Единая фабрика для создания DatabaseConfig и Engine объектов
Централизованное управление подключениями к базе данных
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any

from .resolver import ConnectionConfigResolver, ConnectionConfig
from .validator import DatabaseValidator

logger = logging.getLogger("db_tools.factory")


# ===============================
# Data classes
# ===============================

@dataclass
class DatabaseConnectionInfo:
    """Информация о подключении к базе данных (удобно для логов/отладки)."""
    host: str
    port: int
    database: str
    username: str
    password: str
    environment: str = "unknown"
    additional_params: Dict[str, Any] = field(default_factory=dict)


# ===============================
# Factory
# ===============================

class DatabaseConfigFactory:
    """
    Единая фабрика для создания конфигураций БД и SQLAlchemy Engine.
    Под капотом использует ConnectionConfigResolver и (опционально) DatabaseValidator.
    Совместимо с SQLAlchemy 2.x.
    """

    def __init__(self) -> None:
        self._resolver = ConnectionConfigResolver()
        self._validator: Optional[DatabaseValidator] = None

    # -------- Config creation --------

    def create_from_env(self) -> ConnectionConfig:
        """
        Создать конфигурацию из переменных окружения (.env поддерживается resolver'ом).
        Основной способ для production.
        """
        logger.info("Creating database configuration from environment variables")
        config = self._resolver.get_database_config()
        logger.info(f"Created configuration for environment: {config.environment.value}")
        return config

    def create_from_url(self, database_url: str) -> ConnectionConfig:
        """Создать конфигурацию из URL (postgresql://user:pass@host:port/dbname)."""
        logger.info("Creating database configuration from URL")
        return self._resolver.from_url(database_url)

    def create_from_dict(self, config_dict: Dict[str, Any]) -> ConnectionConfig:
        """Создать конфигурацию из словаря параметров (удобно для тестов)."""
        logger.info("Creating database configuration from dictionary")
        return self._resolver.from_dict(config_dict)

    def create_from_file(self, file_path: str) -> ConnectionConfig:
        """Создать конфигурацию из JSON/YAML файла."""
        logger.info(f"Creating database configuration from file: {file_path}")
        return self._resolver.from_file(file_path)

    def create_with_validation(
        self,
        validate_schema: bool = True,
        auto_repair: bool = False
    ) -> ConnectionConfig:
        """
        Создать конфигурацию с валидацией подключения/схемы.
        Валидация НЕ бросает исключение — даёт предупреждение и возвращает конфиг.

        Args:
            validate_schema: Проверять ли готовность схемы к ETL.
            auto_repair: Пытаться ли автоматически устранять найденные проблемы.
        """
        logger.info("Creating database configuration with validation")
        config = self.create_from_env()

        if validate_schema:
            if self._validator is None:
                self._validator = DatabaseValidator(config)
            result = self._validator.validate_full_stack(auto_repair=auto_repair)
            if not result.get("ready_for_etl", False):
                logger.warning("Database validation issues: %s", result.get("message", "unknown"))
        return config

    # -------- Engine creation --------

    def create_engine_with_resolver(self, **kwargs) -> Any:
        """
        Создать SQLAlchemy Engine с использованием resolver.
        Совместимо с SQLAlchemy 2.x (проверка соединения через exec_driver_sql).

        Args:
            **kwargs: Доп. параметры, проксируются в sqlalchemy.create_engine(...)
        """
        from sqlalchemy import create_engine, text  # text может пригодиться вызывающему коду

        config = self.create_from_env()
        connection_string = config.connection_string

        # Базовые параметры для production (могут быть переопределены через kwargs)
        engine_params: Dict[str, Any] = {
            "pool_size": 5,
            "max_overflow": 10,
            "pool_timeout": 30,
            "pool_recycle": 3600,
            "echo": False,
            # В SA 2.x рекомендуется явно указывать future=True для совместимости
            "future": True,
        }
        engine_params.update(kwargs)

        logger.info("Creating SQLAlchemy engine for %s environment", config.environment)
        engine = create_engine(connection_string, **engine_params)

        # Тест соединения (SA 2.x): используем exec_driver_sql
        try:
            with engine.connect() as conn:
                db_version = conn.exec_driver_sql("SELECT version()").scalar_one()
                logger.info("Engine created successfully, connected to: %s", db_version)
        except Exception as e:
            logger.error("Failed to create engine: %s", e)
            raise

        return engine


# ===============================
# Singleton + convenience
# ===============================

_factory_instance: Optional[DatabaseConfigFactory] = None


def get_database_factory() -> DatabaseConfigFactory:
    """Получить singleton фабрики."""
    global _factory_instance
    if _factory_instance is None:
        _factory_instance = DatabaseConfigFactory()
    return _factory_instance


def create_config_from_env() -> ConnectionConfig:
    """Быстрое создание конфигурации из env."""
    return get_database_factory().create_from_env()


def create_engine_with_resolver(**kwargs) -> Any:
    """Быстрое создание Engine с resolver (совместимо с SA 2.x)."""
    return get_database_factory().create_engine_with_resolver(**kwargs)


def create_validated_config(auto_repair: bool = False) -> ConnectionConfig:
    """Быстрое создание валидированной конфигурации (без исключений при проблемах)."""
    return get_database_factory().create_with_validation(auto_repair=auto_repair)
