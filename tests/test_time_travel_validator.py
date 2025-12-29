"""
Tests for Time Travel Validator
"""

import pytest
from unittest.mock import Mock
from nhl_pipeline.utils.time_travel_validator import (
    TimeTravelValidator,
    ValidationResult
)


@pytest.fixture
def mock_connection():
    """Mock Snowflake connection."""
    conn = Mock()
    cursor = Mock()
    # Mock quote_identifier to just return the identifier with quotes
    cursor.quote_identifier = Mock(side_effect=lambda x: f'"{x}"')
    conn.cursor.return_value = cursor
    return conn, cursor


def test_calculate_change_pct():
    """Test percentage change calculation."""
    validator = TimeTravelValidator(
        connection_params={},
        lookback_minutes=60
    )
    
    # Normal change
    assert validator._calculate_change_pct(110, 100) == 10.0
    assert validator._calculate_change_pct(90, 100) == -10.0
    
    # Zero historical value
    assert validator._calculate_change_pct(100, 0) == float('inf')
    assert validator._calculate_change_pct(0, 0) is None


def test_validate_row_count_pass(mock_connection):
    """Test row count validation that passes."""
    conn, cursor = mock_connection
    cursor.fetchone.return_value = (1000, 950)  # Current: 1000, Historical: 950
    
    validator = TimeTravelValidator(
        connection_params={},
        lookback_minutes=60,
        row_count_threshold=0.20  # 20% threshold
    )
    validator.connect = Mock(return_value=conn)
    
    result = validator._validate_row_count(conn, 'analytics', 'fact_player_game_stats')
    
    assert result.passed is True
    assert result.current_value == 1000
    assert result.historical_value == 950
    assert result.change_pct == pytest.approx(5.26, rel=0.1)


def test_validate_row_count_fail(mock_connection):
    """Test row count validation that fails."""
    conn, cursor = mock_connection
    cursor.fetchone.return_value = (1300, 1000)  # 30% increase
    
    validator = TimeTravelValidator(
        connection_params={},
        lookback_minutes=60,
        row_count_threshold=0.20  # 20% threshold
    )
    validator.connect = Mock(return_value=conn)
    
    result = validator._validate_row_count(conn, 'analytics', 'fact_player_game_stats')
    
    assert result.passed is False
    assert result.change_pct == 30.0
    assert "WARNING" in result.message


def test_validate_row_count_new_table(mock_connection):
    """Test row count validation for new table (historical count was 0)."""
    conn, cursor = mock_connection
    cursor.fetchone.return_value = (1000, 0)  # New table with data
    
    validator = TimeTravelValidator(
        connection_params={},
        lookback_minutes=60,
        row_count_threshold=0.20  # 20% threshold
    )
    validator.connect = Mock(return_value=conn)
    
    result = validator._validate_row_count(conn, 'analytics', 'fact_player_game_stats')
    
    # New table should pass validation
    assert result.passed is True
    assert result.change_pct == float('inf')
    assert "NEW: table had no data historically" in result.message
    assert "WARNING" not in result.message


def test_validate_column_nulls_pass(mock_connection):
    """Test null validation that passes."""
    conn, cursor = mock_connection
    # Current nulls: [5, 10], Historical nulls: [5, 9]
    cursor.fetchone.return_value = (5, 10, 5, 9)
    
    validator = TimeTravelValidator(
        connection_params={},
        lookback_minutes=60,
        null_threshold=0.20  # 20% threshold
    )
    validator.connect = Mock(return_value=conn)
    
    results = validator._validate_column_nulls(
        conn, 
        'analytics', 
        'fact_player_game_stats',
        ['player_id', 'team_id']
    )
    
    assert len(results) == 2
    assert all(r.passed for r in results)


def test_validate_column_nulls_fail(mock_connection):
    """Test null validation that fails on critical ID fields."""
    conn, cursor = mock_connection
    # Current nulls: [50, 0], Historical nulls: [0, 0]
    # game_id nulls increased from 0 to 50 - this is a DATA QUALITY ISSUE
    cursor.fetchone.return_value = (50, 0, 0, 0)
    
    validator = TimeTravelValidator(
        connection_params={},
        lookback_minutes=60,
        null_threshold=0.10  # 10% threshold
    )
    validator.connect = Mock(return_value=conn)
    
    results = validator._validate_column_nulls(
        conn,
        'analytics',
        'fact_player_game_stats', 
        ['game_id', 'player_id']  # Critical IDs that should never be null
    )
    
    assert len(results) == 2
    assert results[0].passed is False  # game_id: nulls appeared (bad!)
    assert results[1].passed is True   # player_id: no change
    assert "WARNING" in results[0].message
    # Should have special message for nulls appearing from zero
    assert "nulls appeared where there were none historically" in results[0].message


def test_validate_column_nulls_empty_list(mock_connection):
    """Test null validation with empty columns list returns empty results."""
    conn, cursor = mock_connection
    
    validator = TimeTravelValidator(
        connection_params={},
        lookback_minutes=60,
        null_threshold=0.10
    )
    validator.connect = Mock(return_value=conn)
    
    results = validator._validate_column_nulls(
        conn,
        'analytics',
        'fact_player_game_stats',
        []  # Empty columns list
    )
    
    # Should return empty list without executing any SQL
    assert results == []
    # Cursor should not have executed any query
    cursor.execute.assert_not_called()


def test_validation_result_dataclass():
    """Test ValidationResult dataclass."""
    result = ValidationResult(
        table_name='analytics.fact_player_game_stats',
        metric='row_count',
        current_value=1000,
        historical_value=950,
        change_pct=5.26,
        passed=True,
        message='Row count: 1,000 (was 950) (+5.3% change)'
    )
    
    assert result.table_name == 'analytics.fact_player_game_stats'
    assert result.metric == 'row_count'
    assert result.passed is True


def test_print_report():
    """Test report printing."""
    results = [
        ValidationResult(
            table_name='analytics.fact_player_game_stats',
            metric='row_count',
            current_value=1000,
            historical_value=950,
            change_pct=5.26,
            passed=True,
            message='Row count: 1,000 (was 950)'
        ),
        ValidationResult(
            table_name='analytics.fact_player_game_stats',
            metric='null_count_player_id',
            current_value=50,
            historical_value=10,
            change_pct=400.0,
            passed=False,
            message='player_id: 50 nulls (was 10) - WARNING'
        ),
    ]
    
    validator = TimeTravelValidator(
        connection_params={},
        lookback_minutes=60
    )
    
    all_passed = validator.print_report(results)
    assert all_passed is False  # One check failed
