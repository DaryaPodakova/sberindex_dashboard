Ключевые моменты:

resolver.py — определяет окружение (docker, localhost, CI), грузит .env, собирает ConnectionConfig с connection_string. Это основной модуль. 

resolver

factory.py — обёртка-фабрика: создаёт ConnectionConfig через resolver, может отдать готовый SQLAlchemy engine или конфиг с валидацией. Там есть функции create_config_from_env() и create_engine_with_resolver(). 

factory

migration_utils.py — специально для замены старого кода: migrate_psycopg2_connection() вернёт готовый psycopg2-коннект через resolver. 

migration_utils

validator.py и health.py — это уже проверки схемы, readiness и автопочинка. 

validator
# Database Tools Module

**Centralized database connectivity and diagnostics for AG Platform Pipeline**

Replaces scattered connection tests and provides unified database infrastructure for all ETL pipelines.

## Overview

The `db_tools` module consolidates database connectivity management and eliminates the need for multiple connection test files across the project. This module implements environment-aware database connections that work seamlessly across Docker, localhost, and production environments.

## Architecture

```
db_tools/
├── __init__.py          # Module exports and version
├── scanner.py           # Network connectivity scanner
├── resolver.py          # Environment-aware connection resolver
└── README.md           # This documentation
```

## Quick Start

```python
from db_tools.resolver import ConnectionConfigResolver
from db_tools.scanner import scan_postgres_ports

# Get database configuration
resolver = ConnectionConfigResolver()
config = resolver.get_database_config()
print(config.connection_string)

# Scan for available PostgreSQL services
scan_results = scan_postgres_ports()
for res in scan_results:
    print(f"{'✅' if res['status'] else '❌'} {res['host']}:{res['port']}")
```

## Components

### scanner.py
**Purpose**: Network connectivity scanner for PostgreSQL services  
**Usage**: Scan multiple hosts/ports to identify accessible PostgreSQL instances

```python
from db_tools.scanner import scan_postgres_ports

# Default scan (localhost, 127.0.0.1, host.docker.internal on ports 5432, 5433)
results = scan_postgres_ports()

# Custom scan
results = scan_postgres_ports(
    hosts=['host.docker.internal', 'localhost'], 
    ports=[5432, 5433, 5434],
    timeout=5
)

for res in results:
    status = "✅" if res['status'] else "❌"
    print(f"{status} {res['host']}:{res['port']}")
```

### resolver.py
**Purpose**: Environment-aware database connection configuration  
**Usage**: Automatically detect environment (Docker/localhost/CI) and provide appropriate connection settings

```python
from db_tools.resolver import ConnectionConfigResolver

resolver = ConnectionConfigResolver()

# Get configuration (auto-detected environment)
config = resolver.get_database_config()
print(f"Host: {config.host}:{config.port}")
print(f"Database: {config.database}")
print(f"Connection String: {config.connection_string}")

# Validate connection
is_valid, message = resolver.validate_connection(config)
print(f"Valid: {is_valid}, Message: {message}")

# Get comprehensive diagnostics
diagnostics = resolver.get_diagnostics()
print(f"Environment: {diagnostics['environment_detected']}")
print(f"Connection Valid: {diagnostics['connection_valid']}")
```

## Environment Detection

The resolver automatically detects your runtime environment:

- **Docker Container** (`host.docker.internal:5432`)
- **Docker Compose** (`postgres:5432`, `db:5432`)
- **Local Development** (`localhost:5432`)
- **CI/CD** (`localhost:5432`)

## Configuration Priority

1. **Environment Variables** (highest priority)
   ```bash
   DB_HOST=custom_host
   DB_PORT=5432
   DB_NAME=my_database
   DB_USER=my_user
   DB_PASSWORD=my_password
   ```

2. **Environment Defaults** (detected automatically)
   - Docker: `host.docker.internal:5432`
   - Local: `localhost:5432`

3. **Fallback Configuration** (may not work)
   - `localhost:5432` with default credentials

## Environment Variables

### Primary Variables (recommended)
```bash
DB_HOST=host.docker.internal
DB_PORT=5432
DB_NAME=multi_platform_data
DB_USER=bot_etl_user2
DB_PASSWORD=your_password
```

### Alternative Variables (legacy support)
```bash
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
POSTGRES_DB=multi_platform_data
POSTGRES_USER=bot_etl_user2
POSTGRES_PASSWORD=your_password
```

## Usage in ETL Pipelines

**Standard Pattern** (use this in all ETL scripts):

```python
from db_tools.resolver import ConnectionConfigResolver
from etl.shared.postgres_downloader import SharedPostgreSQLDownloader

# Get connection configuration
resolver = ConnectionConfigResolver()
config = resolver.get_database_config()

# Use with shared PostgreSQL downloader
downloader = SharedPostgreSQLDownloader(
    host=config.host,
    port=config.port,
    database=config.database,
    user=config.user,
    password=config.password
)
```

## Diagnostics and Troubleshooting

### Run Connection Diagnostics
```bash
# From project root
cd /workspaces/ag_platform_pipline
python -m db_tools.resolver
```

### Expected Output
```
🔍 Connection Configuration Resolver Diagnostics
============================================================

📊 Environment Detection:
  Environment: docker_host

🔧 Selected Configuration:
  Host: host.docker.internal
  Port: 5432
  Database: multi_platform_data
  Connection String: postgresql://bot_etl_user2:***@host.docker.internal:5432/multi_platform_data

✅ Connection Validation:
  Status: ✅ VALID
  Message: Connected to PostgreSQL: PostgreSQL 13.x on x86_64...

🌐 Network Tests:
  ❌ localhost:5432
  ❌ 127.0.0.1:5432
  ✅ host.docker.internal:5432
  ✅ host.docker.internal:5432

🔑 Environment Variables:
  DB_HOST: host.docker.internal
  POSTGRES_HOST: Not set
  DB_PORT: 5432
  COMPOSE_PROJECT_NAME: Not set

============================================================
🎉 DATABASE CONNECTION: SUCCESS
   ✅ PostgreSQL is accessible and ready for ETL operations
```

### Common Issues

**❌ "Cannot connect to localhost:5432"**
- **Solution**: You're in Docker environment, use `host.docker.internal`
- **Fix**: Set `DB_HOST=host.docker.internal` in `.env`

**❌ "python-dotenv parsing error"**
- **Solution**: Check `.env` file for malformed lines
- **Fix**: Remove Python code comments from `.env`

**❌ "Connection timeout"**
- **Solution**: PostgreSQL service not running
- **Fix**: Start PostgreSQL service or check Docker containers

## Migration Guide

### From Legacy Connection Patterns

**Old Pattern** (don't use):
```python
# ❌ Don't do this anymore
import psycopg2
conn = psycopg2.connect(
    host="localhost",  # Hard-coded, breaks in Docker
    port=5432,
    database="mydb",
    user="user",
    password="pass"
)
```

**New Pattern** (use this):
```python
# ✅ Use this pattern
from db_tools.resolver import ConnectionConfigResolver

resolver = ConnectionConfigResolver()
config = resolver.get_database_config()

import psycopg2
conn = psycopg2.connect(config.connection_string)
```

### Environment Migration (Docker ↔ Localhost)

The resolver handles environment transitions automatically:
- **In Docker**: Uses `host.docker.internal:5432`
- **On Localhost**: Uses `localhost:5432`  
- **In CI/CD**: Uses environment-specific configuration

No code changes required when moving between environments.

## Integration with Shared Components

### With SharedPostgreSQLDownloader
```python
from db_tools.resolver import ConnectionConfigResolver
from etl.shared.postgres_downloader import SharedPostgreSQLDownloader

resolver = ConnectionConfigResolver()
config = resolver.get_database_config()

downloader = SharedPostgreSQLDownloader(
    host=config.host,
    port=config.port, 
    database=config.database,
    user=config.user,
    password=config.password
)
```

### With SharedSchemaValidator
```python
from db_tools.resolver import ConnectionConfigResolver
from etl.shared.schema_validator import SharedSchemaValidator

resolver = ConnectionConfigResolver()
config = resolver.get_database_config()

validator = SharedSchemaValidator({
    'host': config.host,
    'port': config.port,
    'database': config.database,
    'user': config.user, 
    'password': config.password
})
```

## Development Guidelines

### For ETL Developers

1. **Always use `db_tools.resolver`** for database connections
2. **Never hard-code** host/port/credentials in ETL scripts  
3. **Test connection** before ETL operations using `validate_connection()`
4. **Use shared components** (SharedPostgreSQLDownloader, SharedSchemaValidator)

### For Platform Integration

```python
# Standard ETL script template
from db_tools.resolver import ConnectionConfigResolver
from etl.shared.postgres_downloader import SharedPostgreSQLDownloader
from etl.shared.schema_validator import SharedSchemaValidator

def main():
    # Get connection configuration
    resolver = ConnectionConfigResolver()
    config = resolver.get_database_config()
    
    # Validate connection before proceeding
    is_valid, message = resolver.validate_connection(config)
    if not is_valid:
        print(f"❌ Database connection failed: {message}")
        return False
    
    # Initialize shared components
    downloader = SharedPostgreSQLDownloader(/* config params */)
    validator = SharedSchemaValidator(/* config */)
    
    # Your ETL logic here
    # ...
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
```

## File Replacement Map

This module replaces the following scattered files:

| Old File | New Location | Status |
|----------|--------------|--------|
| `simple_connection_test.py` | `db_tools/scanner.py` | ✅ Replaced |
| `connection_config_resolver.py` | `db_tools/resolver.py` | ✅ Replaced |
| `test_postgres_connection.py` | `db_tools/resolver.py` (diagnostics) | ✅ Replaced |
| Various connection testers | `db_tools/` module | ✅ Consolidated |

## Version History

- **v1.0.0**: Initial release with scanner and resolver
  - Environment-aware connection resolution
  - Network connectivity scanning
  - Comprehensive diagnostics
  - Docker/localhost seamless migration

## Contributing

When adding new database tools:

1. Follow the existing pattern in `scanner.py` and `resolver.py`
2. Add comprehensive docstrings and type hints
3. Include usage examples in docstrings
4. Update `__init__.py` exports
5. Add tests (use `db_tools` for testing, don't create separate test files)
6. Update this README with new functionality

## Support

For connection issues:
1. Run diagnostics: `python -m db_tools.resolver`
2. Check `.env` file format and variables
3. Verify PostgreSQL service is running
4. Review network connectivity with `db_tools.scanner`

**Remember**: Use `db_tools` for ALL database connectivity needs. Stop creating new connection test files!