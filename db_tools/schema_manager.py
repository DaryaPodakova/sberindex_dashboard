#!/usr/bin/env python3
"""
Centralized DDL Management System
Implements dynamic schema management with proper error handling and validation
Following architectural patterns for database management
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import json

from .connection_pool_legacy import DatabaseManager
from .schema_validator import OrchestratorCompatibilityChecker, validate_all_tables

logger = logging.getLogger(__name__)


@dataclass
class SchemaValidationResult:
    """Result of schema validation operation."""
    valid: bool
    table_name: str
    expected_columns: List[str]
    missing_columns: List[str]
    extra_columns: List[str]
    errors: List[str]
    metadata: Dict[str, Any]


@dataclass
class DDLExecutionResult:
    """Result of DDL execution operation."""
    success: bool
    table_name: str
    operation: str
    execution_time: float
    rows_affected: int
    errors: List[str]
    metadata: Dict[str, Any]


class DDLManager:
    """
    Centralized DDL management with dynamic loading and validation.
    Implements proper connection management and error handling.
    """

    def __init__(self, connection_params: Dict[str, str], sql_directory: Path = None):
        """
        Initialize DDL manager with database connection and SQL directory.

        Args:
            connection_params: Database connection parameters
            sql_directory: Path to SQL DDL files (defaults to etl/sql)
        """
        self.connection_params = connection_params
        self.sql_directory = sql_directory or Path("etl/sql")
        self.ddl_cache: Dict[str, str] = {}
        self.db_manager = DatabaseManager(connection_params)
        self.validator = OrchestratorCompatibilityChecker(self.db_manager)
        self._validate_sql_directory()

    def _validate_sql_directory(self) -> None:
        """Validate SQL directory exists and contains DDL files."""
        if not self.sql_directory.exists():
            raise FileNotFoundError(f"SQL directory not found: {self.sql_directory}")

        ddl_files = list(self.sql_directory.glob("*ddl*.sql"))
        if not ddl_files:
            raise FileNotFoundError(f"No DDL files found in: {self.sql_directory}")

        logger.info(f"DDL Manager initialized with {len(ddl_files)} DDL files")

    def _get_connection(self) -> psycopg2.extensions.connection:
        """Create database connection with proper error handling."""
        try:
            # Convert connection_params to psycopg2 format if needed
            psycopg2_params = self.connection_params.copy()
            if 'database' in psycopg2_params and 'dbname' not in psycopg2_params:
                psycopg2_params['dbname'] = psycopg2_params.pop('database')

            conn = psycopg2.connect(**psycopg2_params)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            return conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def _clean_ddl_content(self, content: str) -> str:
        """Clean DDL content by removing BOM and fixing orphaned comments."""
        # Remove BOM if present
        if content.startswith('\ufeff'):
            content = content[1:]
            logger.debug("Removed BOM from DDL content")

        # Remove leading orphaned comment closers
        lines = content.split('\n')
        cleaned_lines = []

        for line in lines:
            stripped_line = line.strip()
            # Skip lines that are just orphaned comment closers
            if stripped_line == '*/' or stripped_line.startswith('*/'):
                logger.warning(f"Removed orphaned comment closer: {stripped_line}")
                continue
            cleaned_lines.append(line)

        return '\n'.join(cleaned_lines)

    def load_ddl_file(self, file_path: Path) -> str:
        """Load DDL content from file with caching and cleaning."""
        file_key = str(file_path)

        if file_key not in self.ddl_cache:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                # Clean the content before caching
                content = self._clean_ddl_content(content)
                self.ddl_cache[file_key] = content
                logger.debug(f"Loaded and cleaned DDL file: {file_path}")
            except Exception as e:
                logger.error(f"Failed to load DDL file {file_path}: {e}")
                raise

        return self.ddl_cache[file_key]

    def get_ddl_files(self) -> Dict[str, Path]:
        """Get available DDL files mapped by platform/table name with execution order."""
        ddl_files = {}

        # Check for DDL files in multiple patterns
        patterns = ["*ddl*.sql", "*_ddl.sql", "l0_*.sql", "l1_*.sql"]
        found_files = []

        for pattern in patterns:
            found_files.extend(self.sql_directory.glob(pattern))

        # M17: Define execution order for split DDL files (API must run first)
        execution_order = {
            'steam_ddl_api.sql': 0,       # MUST run first (contains watermark + helpers + API tables)
            'steam_ddl_file.sql': 1,      # FILE tables depend on API (shared watermark)
        }

        for file_path in set(found_files):  # Remove duplicates
            filename = file_path.name  # Use full filename for ordering
            filename_stem = file_path.stem

            # M17: Skip deprecated monolithic steam_ddl.sql if split files exist
            if filename == 'steam_ddl.sql':
                split_files_exist = any(
                    (self.sql_directory / f).exists()
                    for f in ['steam_ddl_api.sql', 'steam_ddl_file.sql']
                )
                if split_files_exist:
                    logger.info(f"⚠️  Skipping deprecated {filename} (split files found)")
                    continue

            # M17: Skip deprecated steam_file_dimensions_ddl.sql (merged into steam_ddl_file.sql)
            if filename == 'steam_file_dimensions_ddl.sql':
                if (self.sql_directory / 'steam_ddl_file.sql').exists():
                    logger.info(f"⚠️  Skipping deprecated {filename} (merged into steam_ddl_file.sql)")
                    continue

            # Extract platform from filename (l0_steam_ddl.sql -> steam)
            if 'steam' in filename_stem:
                platform = 'steam'
            elif 'sony' in filename_stem:
                platform = 'sony'
            elif 'epic' in filename_stem:
                platform = 'epic'
            elif 'gog' in filename_stem:
                platform = 'gog'
            elif 'nintendo' in filename_stem:
                platform = 'nintendo'
            elif 'xbox' in filename_stem:
                platform = 'xbox'
            else:
                platform = 'generic'

            # Determine if it's L0 or L1
            layer = 'l1' if 'l1' in filename_stem else 'l0'

            # M17: Handle split DDL files with execution order
            if filename in execution_order:
                # Use execution order as suffix to ensure proper sorting
                order = execution_order[filename]
                key = f"{platform}_{layer}_{order:02d}_{filename_stem}"
            else:
                # Legacy single-file behavior
                key = f"{platform}_{layer}"
                if key in ddl_files and 'optimized' not in filename_stem:
                    continue  # Skip if already have a file for this platform/layer

            ddl_files[key] = file_path

        logger.info(f"Found DDL files: {list(ddl_files.keys())}")
        return ddl_files

    def _split_sql_statements(self, ddl_content: str) -> List[str]:
        """Split SQL content into statements, respecting DO $$ ... $$ blocks and functions."""
        statements = []
        current_statement = ""
        in_dollar_block = False
        dollar_tag = None

        lines = ddl_content.split('\n')

        for line in lines:
            stripped_line = line.strip()

            # Skip empty lines and comments
            if not stripped_line or stripped_line.startswith('--'):
                if current_statement.strip():
                    current_statement += line + '\n'
                continue

            # Check for dollar-quoted strings (DO $$ ... $$ or functions)
            if '$$' in line:
                dollar_positions = []
                i = 0
                while i < len(line) - 1:
                    if line[i:i+2] == '$$':
                        dollar_positions.append(i)
                        i += 2
                    else:
                        i += 1

                for pos in dollar_positions:
                    if not in_dollar_block:
                        # Find the tag (text between $)
                        tag_start = pos + 2
                        tag_end = line.find('$$', tag_start)
                        if tag_end != -1:
                            dollar_tag = line[tag_start:tag_end]
                        else:
                            dollar_tag = ''
                        in_dollar_block = True
                    else:
                        # Check if this closes the current block
                        tag_start = pos + 2
                        tag_end = line.find('$$', tag_start)
                        if tag_end != -1:
                            closing_tag = line[tag_start:tag_end]
                        else:
                            closing_tag = ''

                        if closing_tag == dollar_tag:
                            in_dollar_block = False
                            dollar_tag = None

            current_statement += line + '\n'

            # If we're not in a dollar block and the line ends with ;
            if not in_dollar_block and stripped_line.endswith(';'):
                stmt = current_statement.strip()
                if stmt and not stmt.startswith('--') and not stmt.startswith('/*'):
                    # Remove inline comments but preserve quoted strings
                    cleaned_stmt = self._clean_inline_comments(stmt)
                    if cleaned_stmt:
                        statements.append(cleaned_stmt)
                current_statement = ""

        # Add any remaining content as a statement
        if current_statement.strip():
            stmt = current_statement.strip()
            if stmt and not stmt.startswith('--') and not stmt.startswith('/*'):
                cleaned_stmt = self._clean_inline_comments(stmt)
                if cleaned_stmt:
                    statements.append(cleaned_stmt)

        return statements

    def _clean_inline_comments(self, statement: str) -> str:
        """Remove inline comments while preserving quoted strings."""
        lines = statement.split('\n')
        clean_lines = []

        for line in lines:
            line = line.strip()
            if line and not line.startswith('--'):
                # Remove inline comments but preserve quoted strings
                if '--' in line and not ("'" in line or '"' in line):
                    line = line.split('--')[0].strip()
                if line:
                    clean_lines.append(line)

        return '\n'.join(clean_lines) if clean_lines else ''

    def execute_ddl(self, ddl_content: str, table_name: str = "unknown", file_path: str = "unknown") -> DDLExecutionResult:
        """Execute DDL with proper transaction handling and error reporting."""
        start_time = datetime.now()
        errors = []
        rows_affected = 0

        try:
            conn = self._get_connection()
            # Use AUTOCOMMIT for DDL operations to avoid transaction issues
            old_isolation = conn.isolation_level
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()

            # Use improved SQL statement splitting
            statements = self._split_sql_statements(ddl_content)
            logger.info(f"Processing {len(statements)} SQL statements from {file_path}")

            for stmt_index, statement in enumerate(statements, 1):
                if statement.strip():
                    try:
                        cursor.execute(statement)
                        if cursor.rowcount > 0:
                            rows_affected += cursor.rowcount
                        logger.debug(f"✅ Statement {stmt_index}/{len(statements)} executed successfully")
                    except Exception as stmt_error:
                        # Detailed error logging with context
                        first_line = statement.split('\n')[0][:200]  # First 200 chars
                        error_msg = f"Statement {stmt_index} failed in {file_path}: {stmt_error}"
                        context_msg = f"  Statement preview: {first_line}..."

                        errors.append(error_msg)
                        logger.error(error_msg)
                        logger.error(context_msg)

                        # Enhanced error categorization and handling
                        error_str = str(stmt_error).lower()
                        if "does not exist" in error_str and "relation" in error_str:
                            logger.warning(f"  [WARN] Non-fatal: Relation dependency issue - continuing")
                            continue
                        elif "already exists" in error_str:
                            logger.warning(f"  [WARN] Non-fatal: Object already exists - continuing")
                            continue
                        elif "if not exists" in statement.lower():
                            logger.warning(f"  [WARN] Non-fatal: Idempotent operation failed - continuing")
                            continue
                        elif "duplicate" in error_str:
                            logger.warning(f"  [WARN] Non-fatal: Duplicate object - continuing")
                            continue
                        elif "cannot run inside a transaction" in error_str or "vacuum" in error_str.lower():
                            logger.warning(f"  [WARN] Non-fatal: VACUUM in transaction block - skipping")
                            continue
                        elif "syntax error" in error_str or "unterminated" in error_str:
                            logger.warning(f"  [WARN] Non-fatal: Syntax issue (likely function/DO block) - continuing")
                            continue
                        elif "current transaction" in error_str and "aborted" in error_str:
                            logger.warning(f"  [WARN] Transaction aborted from previous error - continuing")
                            continue
                        else:
                            # Don't raise - just log and continue
                            logger.error(f"  [ERROR] Error encountered but continuing: {error_str[:100]}")
                            continue

            cursor.close()
            # Restore original isolation level
            conn.set_isolation_level(old_isolation)
            conn.close()

            execution_time = (datetime.now() - start_time).total_seconds()

            return DDLExecutionResult(
                success=len(errors) == 0,
                table_name=table_name,
                operation="CREATE_TABLES",
                execution_time=execution_time,
                rows_affected=rows_affected,
                errors=errors,
                metadata={
                    "statements_executed": len(statements),
                    "ddl_length": len(ddl_content),
                    "file_path": file_path,
                    "successful_statements": len(statements) - len(errors)
                }
            )

        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"DDL execution failed for {table_name} in {file_path}: {e}"
            errors.append(error_msg)
            logger.error(error_msg)

            # Add detailed context for debugging
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")

            return DDLExecutionResult(
                success=False,
                table_name=table_name,
                operation="CREATE_TABLES",
                execution_time=execution_time,
                rows_affected=rows_affected,
                errors=errors,
                metadata={
                    "exception": str(e),
                    "file_path": file_path,
                    "traceback": traceback.format_exc()
                }
            )

    def validate_table_schema(self, table_name: str, expected_columns: List[str]) -> SchemaValidationResult:
        """Validate table schema against expected columns."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Get actual table columns
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (table_name,))

            actual_columns = [row[0] for row in cursor.fetchall()]

            cursor.close()
            conn.close()

            # Compare columns
            missing_columns = [col for col in expected_columns if col not in actual_columns]
            extra_columns = [col for col in actual_columns if col not in expected_columns]

            is_valid = len(missing_columns) == 0 and len(extra_columns) == 0

            return SchemaValidationResult(
                valid=is_valid,
                table_name=table_name,
                expected_columns=expected_columns,
                missing_columns=missing_columns,
                extra_columns=extra_columns,
                errors=[],
                metadata={
                    "actual_columns": actual_columns,
                    "total_expected": len(expected_columns),
                    "total_actual": len(actual_columns)
                }
            )

        except Exception as e:
            logger.error(f"Schema validation failed for {table_name}: {e}")
            return SchemaValidationResult(
                valid=False,
                table_name=table_name,
                expected_columns=expected_columns,
                missing_columns=[],
                extra_columns=[],
                errors=[str(e)],
                metadata={}
            )

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get comprehensive table information for debugging."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Get table info
            cursor.execute("""
                SELECT
                    schemaname, tablename, hasindexes, hasrules, hastriggers,
                    pg_size_pretty(pg_total_relation_size(quote_ident(tablename))) as size
                FROM pg_tables
                WHERE tablename = %s
            """, (table_name,))

            table_info = cursor.fetchone()
            if not table_info:
                return {"exists": False, "error": f"Table {table_name} not found"}

            # Get column info
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (table_name,))

            columns = cursor.fetchall()

            # Get index info
            cursor.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = %s AND schemaname = 'public'
            """, (table_name,))

            indexes = cursor.fetchall()

            cursor.close()
            conn.close()

            return {
                "exists": True,
                "schema": table_info[0],
                "name": table_info[1],
                "has_indexes": table_info[2],
                "has_rules": table_info[3],
                "has_triggers": table_info[4],
                "size": table_info[5],
                "columns": [{"name": col[0], "type": col[1], "nullable": col[2], "default": col[3]} for col in columns],
                "indexes": [{"name": idx[0], "definition": idx[1]} for idx in indexes],
                "column_count": len(columns),
                "index_count": len(indexes)
            }

        except Exception as e:
            logger.error(f"Failed to get table info for {table_name}: {e}")
            return {"exists": False, "error": str(e)}

    def get_all_platform_tables(self) -> List[str]:
        """Get all platform-related tables from the database."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Query all tables related to ETL platforms
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND (
                    table_name LIKE 'l0_%'
                    OR table_name LIKE 'l1_%'
                    OR table_name LIKE '%steam%'
                    OR table_name LIKE '%sony%'
                    OR table_name LIKE '%epic%'
                    OR table_name LIKE '%gog%'
                    OR table_name LIKE '%nintendo%'
                    OR table_name LIKE '%xbox%'
                    OR table_name LIKE '%watermark%'
                    OR table_name LIKE '%registry%'
                    OR table_name LIKE '%mapping%'
                    OR table_name LIKE '%_log'
                )
                ORDER BY table_name
            """)

            tables = [row[0] for row in cursor.fetchall()]

            cursor.close()
            conn.close()

            return tables

        except Exception as e:
            logger.error(f"Failed to get all platform tables: {e}")
            return []

    def create_all_tables(self) -> List[DDLExecutionResult]:
        """Create all tables using available DDL files in correct dependency order."""
        results = []
        ddl_files = self.get_ddl_files()

        # M17: Sort by key to ensure execution order (steam_l0_00_shared → steam_l0_01_api → steam_l0_02_file)
        sorted_ddl_files = sorted(ddl_files.items(), key=lambda x: x[0])

        for platform_layer, file_path in sorted_ddl_files:
            try:
                logger.info(f"Creating tables from {platform_layer}: {file_path}")
                ddl_content = self.load_ddl_file(file_path)
                result = self.execute_ddl(ddl_content, platform_layer, str(file_path))
                results.append(result)

                if result.success:
                    logger.info(f"✅ Successfully created {platform_layer} tables")
                else:
                    logger.warning(f"⚠️ Issues creating {platform_layer} tables: {result.errors}")

            except Exception as e:
                logger.error(f"❌ Failed to process {platform_layer}: {e}")
                results.append(DDLExecutionResult(
                    success=False,
                    table_name=platform_layer,
                    operation="CREATE_TABLES",
                    execution_time=0.0,
                    rows_affected=0,
                    errors=[str(e)],
                    metadata={"file_path": str(file_path)}
                ))

        return results

    def verify_table_compatibility(self, table_name: str, platform: str) -> Dict[str, Any]:
        """Verify table schema is compatible with orchestrator expectations."""
        # Get actual table structure first
        table_info = self.get_table_info(table_name)
        if not table_info.get("exists", False):
            return {"compatible": False, "error": f"Table {table_name} does not exist"}

        actual_columns = [col["name"] for col in table_info.get("columns", [])]

        # Define flexible compatibility rules based on actual table schema
        compatibility_rules = {
            "steam": {
                "l0_steam_raw": {
                    # Core required columns (flexible to support both old and optimized schemas)
                    "required": ["id", "source_name", "payload"],
                    # Optional columns that may exist in optimized schema
                    "optional": ["batch_id", "report_type", "report_period", "created_at", "loaded_at",
                                "file_name", "file_path", "content", "metadata", "extraction_date"]
                },
                "l1_steam_sales_country": {
                    "required": ["report_period", "country", "sku", "platform"],
                    "optional": ["net_units", "net_usd"]
                },
                "l1_steam_revenue_share_country": {
                    "required": ["report_period", "country", "sku", "platform"],
                    "optional": ["revenue_share_usd"]
                },
                "steam_highwatermark": {
                    "required": ["source_name", "watermark_date", "grace_days", "updated_at"],
                    "optional": ["notes"]
                }
            },
            "sony": {
                "l0_sony_raw": {
                    "required": ["id", "source_name", "payload"],
                    "optional": ["batch_id", "extraction_date", "created_at", "event_date", "product_id"]
                },
                "l1_sony_financials": {
                    "required": ["id"],
                    "optional": ["batch_id", "transaction_date", "product_id", "gross_revenue_usd"]
                }
            }
        }

        rules = compatibility_rules.get(platform, {}).get(table_name, {})
        if not rules:
            return {"compatible": True, "warning": f"No compatibility rules defined for {platform}.{table_name}"}

        required_columns = rules.get("required", [])
        optional_columns = rules.get("optional", [])

        # Check for required columns
        missing_required = [col for col in required_columns if col not in actual_columns]

        # Determine compatibility
        is_compatible = len(missing_required) == 0

        return {
            "compatible": is_compatible,
            "missing_required_columns": missing_required,
            "has_batch_id": "batch_id" in actual_columns,
            "has_extraction_date": "extraction_date" in actual_columns,
            "schema_type": "optimized" if "batch_id" in actual_columns else "legacy",
            "validation_errors": [f"Missing required columns: {missing_required}"] if missing_required else [],
            "table_info": table_info,
            "actual_columns": actual_columns,
            "required_columns": required_columns,
            "optional_columns": optional_columns
        }


# Import here to avoid circular imports
from datetime import datetime