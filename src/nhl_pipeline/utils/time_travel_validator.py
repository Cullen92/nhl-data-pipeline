"""
Time Travel Data Quality Validator

Compares current table state with historical snapshots using Snowflake Time Travel
to detect unexpected changes in data quality metrics.
"""

import logging
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import snowflake.connector

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a single validation check."""
    table_name: str
    metric: str
    current_value: Any
    historical_value: Any
    change_pct: Optional[float]
    passed: bool
    message: str


class TimeTravelValidator:
    """Validates data quality by comparing current state with historical snapshots."""
    
    def __init__(
        self, 
        connection_params: Dict[str, str],
        lookback_minutes: int = 60,
        row_count_threshold: float = 0.20,  # 20% change threshold
        null_threshold: float = 0.10  # 10% change in nulls
    ):
        """
        Initialize validator.
        
        Args:
            connection_params: Snowflake connection parameters
            lookback_minutes: How far back to compare (default 60 minutes)
            row_count_threshold: Max acceptable % change in row count
            null_threshold: Max acceptable % change in null counts
        """
        self.connection_params = connection_params
        self.lookback_seconds = lookback_minutes * 60
        self.row_count_threshold = row_count_threshold
        self.null_threshold = null_threshold
        
    def connect(self) -> snowflake.connector.SnowflakeConnection:
        """Create Snowflake connection."""
        return snowflake.connector.connect(**self.connection_params)
    
    def _calculate_change_pct(self, current: float, historical: float) -> Optional[float]:
        """Calculate percentage change between current and historical values."""
        if historical == 0:
            return None if current == 0 else float('inf')
        return ((current - historical) / historical) * 100
    
    def _validate_row_count(
        self, 
        conn: snowflake.connector.SnowflakeConnection,
        schema: str,
        table: str
    ) -> ValidationResult:
        """Compare current row count with historical."""
        cursor = conn.cursor()
        try:
            query = f"""
            SELECT 
                (SELECT COUNT(*) FROM {schema}.{table}) AS current_count,
                (SELECT COUNT(*) FROM {schema}.{table} 
                 AT(OFFSET => -{self.lookback_seconds})) AS historical_count
            """

            cursor.execute(query)
            result = cursor.fetchone()
        finally:
            cursor.close()
        
        current_count = result[0]
        historical_count = result[1]
        change_pct = self._calculate_change_pct(current_count, historical_count)
        
        passed = True
        message = f"Row count: {current_count:,} (was {historical_count:,})"
        
        # Special handling for infinity (historical count was 0)
        if change_pct == float('inf'):
            # New table with data - this is typically legitimate
            passed = True
            message += " - NEW: table had no data historically"
        elif change_pct is not None and abs(change_pct) > (self.row_count_threshold * 100):
            passed = False
            message += f" - WARNING: {change_pct:+.1f}% change exceeds threshold"
        elif change_pct is not None:
            message += f" ({change_pct:+.1f}% change)"
            
        return ValidationResult(
            table_name=f"{schema}.{table}",
            metric="row_count",
            current_value=current_count,
            historical_value=historical_count,
            change_pct=change_pct,
            passed=passed,
            message=message
        )
    
    def _validate_column_nulls(
        self,
        conn: snowflake.connector.SnowflakeConnection,
        schema: str,
        table: str,
        columns: List[str]
    ) -> List[ValidationResult]:
        """Compare null counts for specified columns."""
        results = []
        
        # Handle edge case: empty columns list would generate malformed SQL
        if not columns:
            logger.warning(f"No columns specified for null validation on {schema}.{table}")
            return results
        
        cursor = conn.cursor()
        try:
            # Build query to check nulls in both current and historical
            null_checks = []
            for col in columns:
                null_checks.append(
                    f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}_nulls"
                )
            
            query = f"""
            WITH current_nulls AS (
                SELECT {', '.join(null_checks)}
                FROM {schema}.{table}
            ),
            historical_nulls AS (
                SELECT {', '.join(null_checks)}
                FROM {schema}.{table} AT(OFFSET => -{self.lookback_seconds})
            )
            SELECT 
                {', '.join([f"c.{col}_nulls" for col in columns])},
                {', '.join([f"h.{col}_nulls" for col in columns])}
            FROM current_nulls c
            CROSS JOIN historical_nulls h
            """

            cursor.execute(query)
            result = cursor.fetchone()
        finally:
            cursor.close()
        
        # Parse results for each column
        mid_point = len(columns)
        for idx, col in enumerate(columns):
            current_nulls = result[idx]
            historical_nulls = result[mid_point + idx]
            change_pct = self._calculate_change_pct(current_nulls, historical_nulls)
            
            passed = True
            message = f"{col}: {current_nulls:,} nulls (was {historical_nulls:,})"
            
            # Special handling for infinity (historical count was 0)
            if change_pct == float('inf'):
                # Nulls appeared where there were none - this could be a data quality issue
                passed = False
                message += " - WARNING: nulls appeared where there were none historically"
            elif change_pct is not None and abs(change_pct) > (self.null_threshold * 100):
                passed = False
                message += f" - WARNING: {change_pct:+.1f}% change in nulls"
            elif change_pct is not None and change_pct != 0:
                message += f" ({change_pct:+.1f}% change)"
            
            results.append(ValidationResult(
                table_name=f"{schema}.{table}",
                metric=f"null_count_{col}",
                current_value=current_nulls,
                historical_value=historical_nulls,
                change_pct=change_pct,
                passed=passed,
                message=message
            ))
        
        return results
    
    def validate_table(
        self,
        schema: str,
        table: str,
        columns_to_check: Optional[List[str]] = None
    ) -> List[ValidationResult]:
        """
        Run all validations on a table.
        
        Args:
            schema: Schema name
            table: Table name
            columns_to_check: List of columns to check for nulls (optional)
            
        Returns:
            List of validation results
        """
        results = []
        
        try:
            conn = self.connect()
            
            # Always check row count
            results.append(self._validate_row_count(conn, schema, table))
            
            # Check column nulls if specified
            if columns_to_check:
                results.extend(
                    self._validate_column_nulls(conn, schema, table, columns_to_check)
                )
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error validating {schema}.{table}: {e}")
            results.append(ValidationResult(
                table_name=f"{schema}.{table}",
                metric="validation_error",
                current_value=None,
                historical_value=None,
                change_pct=None,
                passed=False,
                message=f"ERROR: {str(e)}"
            ))
        
        return results
    
    def validate_tables(
        self,
        tables_config: List[Dict[str, Any]]
    ) -> List[ValidationResult]:
        """
        Validate multiple tables.
        
        Args:
            tables_config: List of dicts with 'schema', 'table', and optional 'columns'
                Example: [
                    {'schema': 'analytics', 'table': 'fact_player_game_stats', 
                     'columns': ['player_id', 'goals']},
                    {'schema': 'staging', 'table': 'stg_games'}
                ]
        
        Returns:
            List of all validation results
        """
        all_results = []
        
        for config in tables_config:
            schema = config['schema']
            table = config['table']
            columns = config.get('columns')
            
            logger.info(f"Validating {schema}.{table}...")
            results = self.validate_table(schema, table, columns)
            all_results.extend(results)
        
        return all_results
    
    def print_report(self, results: List[ValidationResult]) -> bool:
        """
        Print validation report and return overall pass/fail.
        
        Returns:
            True if all validations passed, False otherwise
        """
        print("\n" + "="*80)
        print("TIME TRAVEL DATA QUALITY VALIDATION REPORT")
        print(f"Lookback: {self.lookback_seconds // 60} minutes")
        print("="*80 + "\n")
        
        passed_count = sum(1 for r in results if r.passed)
        failed_count = len(results) - passed_count
        
        # Group by table
        by_table = {}
        for result in results:
            if result.table_name not in by_table:
                by_table[result.table_name] = []
            by_table[result.table_name].append(result)
        
        # Print results by table
        for table_name, table_results in by_table.items():
            print(f"\n📊 {table_name}")
            print("-" * 80)
            
            for result in table_results:
                status = "✅" if result.passed else "❌"
                print(f"  {status} {result.message}")
        
        # Summary
        print("\n" + "="*80)
        print(f"SUMMARY: {passed_count} passed, {failed_count} failed")
        print("="*80 + "\n")
        
        return failed_count == 0


def main():
    """CLI entry point."""
    import os
    import sys
    import yaml
    from pathlib import Path
    
    # Connection params from environment/config
    connection_params = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'NHL_WH'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'NHL_DB'),
        'role': os.getenv('SNOWFLAKE_ROLE', 'TRANSFORMER')
    }

    # Validate required Snowflake credentials early for clearer errors
    required_env_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD']
    missing_env_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_env_vars:
        print(
            "❌ Missing required Snowflake environment variables: "
            + ", ".join(missing_env_vars)
        )
        print(
            "Please set these variables (e.g., in your shell, .env file, or CI configuration) "
            "before running the time travel validator."
        )
        sys.exit(1)
    
    # Load config from YAML
    config_path = Path(__file__).parent.parent.parent.parent / 'config' / 'data_validation.yml'
    
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"⚠️  Config file not found: {config_path}")
        print("Using default configuration...")
        config = {
            'lookback_minutes': 60,
            'row_count_threshold': 0.20,
            'null_threshold': 0.10,
            'tables': [
                {
                    'schema': 'staging',
                    'table': 'stg_games',
                    'columns': ['game_id', 'home_team_id', 'away_team_id']
                },
                {
                    'schema': 'staging',
                    'table': 'stg_player_game_stats',
                    'columns': ['player_id', 'game_id', 'team_id']
                },
                {
                    'schema': 'analytics',
                    'table': 'fact_player_game_stats',
                    'columns': ['player_id', 'game_id']
                }
            ]
        }
    
    # Environment variables override config file
    lookback_minutes = int(os.getenv('LOOKBACK_MINUTES', config.get('lookback_minutes', 60)))
    row_count_threshold = float(os.getenv('ROW_COUNT_THRESHOLD', config.get('row_count_threshold', 0.20)))
    null_threshold = float(os.getenv('NULL_THRESHOLD', config.get('null_threshold', 0.10)))
    
    # Run validation
    validator = TimeTravelValidator(
        connection_params=connection_params,
        lookback_minutes=lookback_minutes,
        row_count_threshold=row_count_threshold,
        null_threshold=null_threshold
    )
    
    results = validator.validate_tables(config['tables'])
    all_passed = validator.print_report(results)
    
    # Exit with appropriate code for CI
    sys.exit(0 if all_passed else 1)


if __name__ == '__main__':
    main()
