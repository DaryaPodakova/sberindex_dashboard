#!/usr/bin/env python3
"""
Enhanced Schema Validation System
Implements comprehensive table schema validation with orchestrator compatibility checks
Following architectural patterns for database schema management
"""

import logging
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass
from enum import Enum
import re

from .connection_pool_legacy import DatabaseManager

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Validation issue severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationIssue:
    """Individual validation issue."""
    severity: ValidationSeverity
    category: str
    message: str
    table_name: str
    column_name: Optional[str] = None
    expected_value: Optional[str] = None
    actual_value: Optional[str] = None
    metadata: Dict[str, Any] = None


@dataclass
class ColumnDefinition:
    """Expected column definition."""
    name: str
    data_type: str
    nullable: bool = True
    default_value: Optional[str] = None
    constraints: List[str] = None
    description: Optional[str] = None


@dataclass
class TableSchema:
    """Complete table schema definition."""
    name: str
    columns: List[ColumnDefinition]
    indexes: List[str] = None
    constraints: List[str] = None
    description: Optional[str] = None


@dataclass
class ValidationReport:
    """Complete validation report."""
    table_name: str
    valid: bool
    issues: List[ValidationIssue]
    compatibility_score: float
    missing_columns: List[str]
    extra_columns: List[str]
    type_mismatches: List[Tuple[str, str, str]]  # column, expected, actual
    metadata: Dict[str, Any]


class OrchestratorCompatibilityChecker:
    """
    Checks table schema compatibility with ETL orchestrators.
    Validates that tables have required columns for business operations.
    """

    # Define orchestrator requirements
    ORCHESTRATOR_REQUIREMENTS = {
        "steam": {
            "l0_steam_raw": {
                "required_columns": [
                    "id", "source_name", "report_type", "report_period",
                    "batch_id", "payload", "created_at", "loaded_at"
                ],
                "optional_columns": [
                    "file_name", "file_path", "content", "metadata", "extraction_date"
                ],
                "column_types": {
                    "id": "bigint",
                    "source_name": "text",
                    "report_type": "text",
                    "report_period": "date",
                    "batch_id": "text",
                    "payload": "jsonb",
                    "created_at": "timestamp with time zone",
                    "loaded_at": "timestamp with time zone"
                },
                "business_rules": [
                    "payload must contain country, sku, platform fields",
                    "batch_id should follow steam_batch_* pattern",
                    "source_name should be 'steam_api'"
                ]
            },
            "l1_steam_sales_country": {
                "required_columns": [
                    "report_period", "country", "sku", "platform",
                    "net_units", "net_usd", "legal_entity"
                ],
                "optional_columns": ["currency", "loaded_at"],
                "column_types": {
                    "report_period": "date",
                    "country": "text",
                    "sku": "text",
                    "platform": "text",
                    "net_units": "bigint",
                    "net_usd": "numeric",
                    "legal_entity": "text"
                }
            },
            "l1_steam_revenue_share_country": {
                "required_columns": [
                    "report_period", "country", "sku", "platform",
                    "revenue_share_usd", "legal_entity"
                ],
                "column_types": {
                    "revenue_share_usd": "numeric"
                }
            },
            "steam_highwatermark": {
                "required_columns": [
                    "source_name", "watermark_date", "grace_days", "updated_at"
                ],
                "optional_columns": [
                    "notes"
                ],
                "column_types": {
                    "source_name": "text",
                    "watermark_date": "date",
                    "grace_days": "smallint",
                    "updated_at": "timestamp with time zone"
                }
            }
        },
        "sony": {
            "l0_sony_raw": {
                "required_columns": [
                    "id", "source_name", "batch_id", "extraction_date",
                    "payload", "created_at"
                ],
                "optional_columns": [
                    "file_name", "file_path", "record_hash", "content",
                    "report_type", "report_period", "legal_entity", "loaded_at"
                ],
                "column_types": {
                    "id": "bigint",
                    "source_name": "text",
                    "batch_id": "text",
                    "extraction_date": "date",
                    "payload": "jsonb",
                    "created_at": "timestamp with time zone"
                },
                "business_rules": [
                    "payload must contain product_sku, country_code fields",
                    "batch_id should follow sony_batch_* pattern",
                    "source_name should be 'sony_domo'"
                ]
            },
            "l1_sony_financials": {
                "required_columns": [
                    "id", "batch_id", "transaction_date", "product_id",
                    "gross_revenue_usd"
                ],
                "optional_columns": [
                    "extraction_date", "event_date", "product_name",
                    "country_code", "country_name", "legal_entity",
                    "net_revenue_usd", "currency", "units_sold",
                    "created_at", "processed_at"
                ],
                "column_types": {
                    "gross_revenue_usd": "numeric",
                    "transaction_date": "date"
                }
            }
        }
    }

    def __init__(self, db_manager: DatabaseManager):
        """Initialize compatibility checker with database manager."""
        self.db_manager = db_manager

    def check_table_compatibility(
        self,
        table_name: str,
        platform: str
    ) -> ValidationReport:
        """
        Check table compatibility with orchestrator requirements.

        Args:
            table_name: Name of table to validate
            platform: Platform (steam, sony, etc.)

        Returns:
            Validation report with compatibility analysis
        """
        issues = []
        compatibility_score = 0.0
        missing_columns = []
        extra_columns = []
        type_mismatches = []

        try:
            # Get table schema requirements
            requirements = self.ORCHESTRATOR_REQUIREMENTS.get(platform, {}).get(table_name, {})
            if not requirements:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.WARNING,
                    category="compatibility",
                    message=f"No compatibility requirements defined for {platform}.{table_name}",
                    table_name=table_name
                ))
                return ValidationReport(
                    table_name=table_name,
                    valid=True,  # No requirements = compatible by default
                    issues=issues,
                    compatibility_score=1.0,
                    missing_columns=[],
                    extra_columns=[],
                    type_mismatches=[],
                    metadata={"warning": "No requirements defined"}
                )

            # Get actual table schema
            actual_schema = self._get_table_schema(table_name)
            if not actual_schema:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.CRITICAL,
                    category="existence",
                    message=f"Table {table_name} does not exist",
                    table_name=table_name
                ))
                return ValidationReport(
                    table_name=table_name,
                    valid=False,
                    issues=issues,
                    compatibility_score=0.0,
                    missing_columns=requirements.get("required_columns", []),
                    extra_columns=[],
                    type_mismatches=[],
                    metadata={"error": "table_not_found"}
                )

            actual_columns = {col['name']: col for col in actual_schema['columns']}
            actual_column_names = set(actual_columns.keys())

            # Check required columns
            required_columns = set(requirements.get("required_columns", []))
            missing_columns = list(required_columns - actual_column_names)

            for missing_col in missing_columns:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    category="missing_column",
                    message=f"Required column '{missing_col}' is missing",
                    table_name=table_name,
                    column_name=missing_col
                ))

            # Check optional columns (informational)
            optional_columns = set(requirements.get("optional_columns", []))
            missing_optional = list(optional_columns - actual_column_names)
            for missing_col in missing_optional:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.INFO,
                    category="missing_optional_column",
                    message=f"Optional column '{missing_col}' is missing",
                    table_name=table_name,
                    column_name=missing_col
                ))

            # Check for extra columns (might indicate schema drift)
            expected_columns = required_columns | optional_columns
            extra_columns = list(actual_column_names - expected_columns)
            for extra_col in extra_columns:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.WARNING,
                    category="extra_column",
                    message=f"Unexpected column '{extra_col}' found",
                    table_name=table_name,
                    column_name=extra_col
                ))

            # Check column types
            expected_types = requirements.get("column_types", {})
            for col_name, expected_type in expected_types.items():
                if col_name in actual_columns:
                    actual_type = actual_columns[col_name]['data_type']
                    if not self._types_compatible(expected_type, actual_type):
                        type_mismatches.append((col_name, expected_type, actual_type))
                        issues.append(ValidationIssue(
                            severity=ValidationSeverity.ERROR,
                            category="type_mismatch",
                            message=f"Column '{col_name}' type mismatch",
                            table_name=table_name,
                            column_name=col_name,
                            expected_value=expected_type,
                            actual_value=actual_type
                        ))

            # Calculate compatibility score
            total_required = len(required_columns)
            missing_required = len([col for col in missing_columns if col in required_columns])
            type_errors = len(type_mismatches)

            if total_required > 0:
                column_score = (total_required - missing_required) / total_required
                type_score = max(0.0, 1.0 - (type_errors * 0.2))  # Penalty for type mismatches
                compatibility_score = (column_score * 0.7) + (type_score * 0.3)
            else:
                compatibility_score = 1.0

            # Validate business rules
            business_rules = requirements.get("business_rules", [])
            for rule in business_rules:
                self._validate_business_rule(table_name, rule, actual_schema, issues)

            # Determine overall validity
            critical_issues = [issue for issue in issues if issue.severity == ValidationSeverity.CRITICAL]
            error_issues = [issue for issue in issues if issue.severity == ValidationSeverity.ERROR]
            is_valid = len(critical_issues) == 0 and len(missing_columns) == 0

            return ValidationReport(
                table_name=table_name,
                valid=is_valid,
                issues=issues,
                compatibility_score=compatibility_score,
                missing_columns=missing_columns,
                extra_columns=extra_columns,
                type_mismatches=type_mismatches,
                metadata={
                    "platform": platform,
                    "total_columns": len(actual_column_names),
                    "required_columns": len(required_columns),
                    "optional_columns": len(optional_columns),
                    "business_rules_checked": len(business_rules)
                }
            )

        except Exception as e:
            logger.error(f"Compatibility check failed for {table_name}: {e}")
            issues.append(ValidationIssue(
                severity=ValidationSeverity.CRITICAL,
                category="system_error",
                message=f"Validation failed: {e}",
                table_name=table_name
            ))

            return ValidationReport(
                table_name=table_name,
                valid=False,
                issues=issues,
                compatibility_score=0.0,
                missing_columns=[],
                extra_columns=[],
                type_mismatches=[],
                metadata={"error": str(e)}
            )

    def _get_table_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed table schema information."""
        try:
            def get_schema(cursor):
                # Get column information
                cursor.execute("""
                    SELECT
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length,
                        numeric_precision,
                        numeric_scale
                    FROM information_schema.columns
                    WHERE table_name = %s AND table_schema = 'public'
                    ORDER BY ordinal_position
                """, (table_name,))

                columns = []
                for row in cursor.fetchall():
                    columns.append({
                        'name': row[0],
                        'data_type': row[1],
                        'nullable': row[2] == 'YES',
                        'default': row[3],
                        'max_length': row[4],
                        'precision': row[5],
                        'scale': row[6]
                    })

                if not columns:
                    return None

                # Get index information
                cursor.execute("""
                    SELECT indexname, indexdef
                    FROM pg_indexes
                    WHERE tablename = %s AND schemaname = 'public'
                """, (table_name,))

                indexes = [{'name': row[0], 'definition': row[1]} for row in cursor.fetchall()]

                # Get constraints
                cursor.execute("""
                    SELECT constraint_name, constraint_type
                    FROM information_schema.table_constraints
                    WHERE table_name = %s AND table_schema = 'public'
                """, (table_name,))

                constraints = [{'name': row[0], 'type': row[1]} for row in cursor.fetchall()]

                return {
                    'name': table_name,
                    'columns': columns,
                    'indexes': indexes,
                    'constraints': constraints
                }

            return self.db_manager.execute_with_retry(get_schema)

        except Exception as e:
            logger.error(f"Failed to get schema for {table_name}: {e}")
            return None

    def _types_compatible(self, expected: str, actual: str) -> bool:
        """Check if actual column type is compatible with expected type."""
        # Normalize type names
        expected = expected.lower().strip()
        actual = actual.lower().strip()

        # Direct match
        if expected == actual:
            return True

        # Common PostgreSQL type aliases and compatibility
        type_mappings = {
            'bigint': ['bigserial', 'int8', 'bigint'],
            'integer': ['int', 'int4', 'serial', 'integer'],
            'text': ['varchar', 'character varying', 'text', 'character'],
            'timestamp with time zone': ['timestamptz', 'timestamp with time zone'],
            'timestamp without time zone': ['timestamp', 'timestamp without time zone'],
            'jsonb': ['json', 'jsonb'],
            'numeric': ['decimal', 'numeric'],
            'date': ['date']
        }

        # Check if types are in the same compatibility group
        for compatible_types in type_mappings.values():
            if expected in compatible_types and actual in compatible_types:
                return True

        # Check for numeric types with precision/scale
        if 'numeric' in expected or 'decimal' in expected:
            return 'numeric' in actual or 'decimal' in actual

        return False

    def _validate_business_rule(
        self,
        table_name: str,
        rule: str,
        schema: Dict[str, Any],
        issues: List[ValidationIssue]
    ) -> None:
        """Validate business rules against table schema."""
        try:
            # Simple rule validation based on text patterns
            # This could be extended to use a more sophisticated rule engine

            if "payload must contain" in rule.lower():
                # Check if table has payload column and it's JSONB
                payload_column = None
                for col in schema['columns']:
                    if col['name'] == 'payload':
                        payload_column = col
                        break

                if not payload_column:
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.ERROR,
                        category="business_rule",
                        message=f"Business rule violation: {rule}",
                        table_name=table_name,
                        column_name="payload"
                    ))
                elif payload_column['data_type'] != 'jsonb':
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.WARNING,
                        category="business_rule",
                        message=f"Payload column should be JSONB for rule: {rule}",
                        table_name=table_name,
                        column_name="payload"
                    ))

            elif "should follow" in rule.lower() and "pattern" in rule.lower():
                # Pattern validation would require actual data inspection
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.INFO,
                    category="business_rule",
                    message=f"Pattern rule requires data validation: {rule}",
                    table_name=table_name
                ))

            elif "should be" in rule.lower():
                # Default value or constraint validation
                match = re.search(r"(\w+) should be '([^']+)'", rule)
                if match:
                    column_name, expected_value = match.groups()
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.INFO,
                        category="business_rule",
                        message=f"Default value rule for {column_name}: {expected_value}",
                        table_name=table_name,
                        column_name=column_name,
                        expected_value=expected_value
                    ))

        except Exception as e:
            logger.warning(f"Business rule validation failed for '{rule}': {e}")


def validate_all_tables(
    db_manager: DatabaseManager,
    platforms: List[str] = None
) -> Dict[str, ValidationReport]:
    """
    Validate all tables for specified platforms.

    Args:
        db_manager: Database manager instance
        platforms: List of platforms to validate (defaults to all)

    Returns:
        Dictionary mapping table names to validation reports
    """
    if platforms is None:
        platforms = ["steam", "sony"]

    checker = OrchestratorCompatibilityChecker(db_manager)
    results = {}

    for platform in platforms:
        requirements = checker.ORCHESTRATOR_REQUIREMENTS.get(platform, {})

        for table_name in requirements.keys():
            try:
                report = checker.check_table_compatibility(table_name, platform)
                results[f"{platform}.{table_name}"] = report

                if report.valid:
                    logger.info(f"✅ {table_name} ({platform}): Compatible (score: {report.compatibility_score:.2f})")
                else:
                    logger.warning(f"⚠️ {table_name} ({platform}): Issues found (score: {report.compatibility_score:.2f})")

            except Exception as e:
                logger.error(f"❌ Failed to validate {table_name} ({platform}): {e}")

    return results