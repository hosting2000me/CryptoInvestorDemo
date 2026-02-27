"""
Unit tests for Crypto Analytics Service.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import polars as pl
import numpy as np
import json
import os

from core.services.crypto_analytics import CryptoAnalytics
from core.db.delta_controller import DeltaTableController
from tests.fixtures.sample_data import (
    TEST_ADDRESS,
    get_sample_btc_quotes_dataframe,
    get_sample_input_transactions,
    get_sample_output_transactions
)


@pytest.fixture
def mock_db_controller():
    """Create a mock SQLAlchemy controller."""
    controller = Mock()
    # Mock async method to return sync value
    async def mock_get_btc_quotes():
        return get_sample_btc_quotes_dataframe()
    controller.get_btc_quotes = mock_get_btc_quotes
    return controller


@pytest.fixture
def mock_delta_controller():
    """Create a mock Delta Table controller."""
    controller = Mock(spec=DeltaTableController)
    controller.get_input_transactions.return_value = get_sample_input_transactions()
    controller.get_output_transactions.return_value = get_sample_output_transactions()
    return controller


@pytest.fixture
def crypto_analytics(mock_db_controller, mock_delta_controller):
    """Create a CryptoAnalytics service instance."""
    return CryptoAnalytics(mock_db_controller, mock_delta_controller)


class TestCryptoAnalytics:
    """Test suite for CryptoAnalytics."""
    
    def test_init(self, mock_db_controller, mock_delta_controller):
        """Test service initialization."""
        service = CryptoAnalytics(mock_db_controller, mock_delta_controller)
        assert service.db_controller == mock_db_controller
        assert service.delta_controller == mock_delta_controller
    
    def test_calculate_benchmark(self, crypto_analytics):
        """Test benchmark calculation."""
        btc_quotes = get_sample_btc_quotes_dataframe()
        result = crypto_analytics._calculate_benchmark(btc_quotes)
        
        assert hasattr(result, 'sharpe')
        assert hasattr(result, 'drawdown')
        assert hasattr(result, 'profit_pct')
        assert isinstance(result.sharpe, float)
        assert isinstance(result.drawdown, float)
        assert isinstance(result.profit_pct, float)
    
    def test_get_address_transactions(self, crypto_analytics, mock_db_controller, mock_delta_controller):
        """Test calculating address statistics."""
        result = crypto_analytics.get_address_transactions(TEST_ADDRESS)
        
        # Load expected results from file
        expected_file = os.path.join(os.path.dirname(__file__), '..', 'expected_results', 'address_transactions_result.json')
        with open(expected_file, 'r') as f:
            expected = json.load(f)
        
        # Compare numeric values with tolerance
        assert abs(result["profit_pct"] - expected["profit_pct"]) < 0.0001
        assert abs(result["sharpe_ratio"] - expected["sharpe_ratio"]) < 0.0001 if not np.isnan(expected["sharpe_ratio"]) else np.isnan(result["sharpe_ratio"])
        assert abs(result["drawdown"] - expected["drawdown"]) < 0.0001
        assert abs(result["exposure"] - expected["exposure"]) < 0.0001
        assert result["count_days_in_market"] == expected["count_days_in_market"]
        assert abs(result["benchmark_profit"] - expected["benchmark_profit"]) < 0.0001
        assert abs(result["benchmark_sharpe"] - expected["benchmark_sharpe"]) < 0.0001 if not np.isnan(expected["benchmark_sharpe"]) else np.isnan(result["benchmark_sharpe"])
        assert abs(result["benchmark_drawdown"] - expected["benchmark_drawdown"]) < 0.0001
        
        # Compare balance_history DataFrame
        assert len(result["balance_history"]) == len(expected["balance_history"]["date_"])
        for col in expected["balance_history"].keys():
            assert col in result["balance_history"].columns
            if col != "date_":
                for i, val in enumerate(expected["balance_history"][col]):
                    if val is None or (isinstance(val, float) and np.isnan(val)):
                        assert result["balance_history"][col][i] is None or np.isnan(result["balance_history"][col][i])
                    else:
                        assert abs(result["balance_history"][col][i] - val) < 0.0001
        
        # Verify controllers were called
        mock_delta_controller.get_input_transactions.assert_called_once_with(TEST_ADDRESS)
        mock_delta_controller.get_output_transactions.assert_called_once_with(TEST_ADDRESS)
    
    def test_get_address_transactions_no_output_transactions(self, crypto_analytics, mock_delta_controller):
        """Test error handling when no output transactions exist."""
        # Mock empty output transactions
        mock_delta_controller = Mock(spec=DeltaTableController)
        mock_delta_controller.get_input_transactions.return_value = get_sample_input_transactions()
        mock_delta_controller.get_output_transactions.return_value = pl.DataFrame({
            "t_time": [],
            "address": [],
            "t_value": [],
            "t_usdvalue": []
        })
        
        service = CryptoAnalytics(crypto_analytics.db_controller, mock_delta_controller)
        
        with pytest.raises(ValueError, match="No output transactions found"):
            service.get_address_transactions(TEST_ADDRESS)
    
    def test_get_address_balance(self, crypto_analytics, mock_db_controller, mock_delta_controller):
        """Test calculating address balance history."""
        result = crypto_analytics.get_address_balance(TEST_ADDRESS)
        
        # Load expected results from file
        expected_file = os.path.join(os.path.dirname(__file__), '..', 'expected_results', 'address_balance_result.json')
        with open(expected_file, 'r') as f:
            expected = json.load(f)
        
        # Compare DataFrame structure and values
        assert isinstance(result, pl.DataFrame)
        assert "value" in result.columns
        
        # Compare column values
        for col in expected.keys():
            assert col in result.columns
            for i, val in enumerate(expected[col]):
                if val is None or (isinstance(val, float) and np.isnan(val)):
                    assert result[col][i] is None or np.isnan(result[col][i])
                elif col == "date_":
                    # Compare dates as strings
                    assert str(result[col][i]) == val
                else:
                    assert abs(result[col][i] - val) < 0.0001
        
        # Verify controllers were called
        mock_delta_controller.get_input_transactions.assert_called_once_with(TEST_ADDRESS)
        mock_delta_controller.get_output_transactions.assert_called_once_with(TEST_ADDRESS)
    
    def test_get_address_balance_no_output_transactions(self, crypto_analytics, mock_delta_controller):
        """Test error handling when no output transactions exist for balance."""
        mock_delta_controller = Mock(spec=DeltaTableController)
        mock_delta_controller.get_input_transactions.return_value = get_sample_input_transactions()
        mock_delta_controller.get_output_transactions.return_value = pl.DataFrame({
            "t_time": [],
            "address": [],
            "t_value": [],
            "t_usdvalue": []
        })
        
        service = CryptoAnalytics(crypto_analytics.db_controller, mock_delta_controller)
        
        with pytest.raises(ValueError, match="No output transactions found"):
            service.get_address_balance(TEST_ADDRESS)
    
    def test_get_benchmark_metrics(self, crypto_analytics, mock_db_controller):
        """Test getting benchmark metrics."""
        result = crypto_analytics.get_benchmark_metrics()
        
        # Load expected results from file
        expected_file = os.path.join(os.path.dirname(__file__), '..', 'expected_results', 'benchmark_metrics_result.json')
        with open(expected_file, 'r') as f:
            expected = json.load(f)
        
        # Compare values with tolerance
        assert abs(result.sharpe - expected["sharpe"]) < 0.0001 if not np.isnan(expected["sharpe"]) else np.isnan(result.sharpe)
        assert abs(result.drawdown - expected["drawdown"]) < 0.0001
        assert abs(result.profit_pct - expected["profit_pct"]) < 0.0001
    
    def test_get_top_addresses_by_profit(self, crypto_analytics, mock_delta_controller):
        """Test getting top addresses by profit."""
        # Mock PNL stats
        mock_pnl_stats = pl.DataFrame({
            "date_": ["2023-10-01", "2023-10-01"],
            "address": ["bc1qaddress1", "bc1qaddress2"],
            "profit2btc": [50000.0, 45000.0],
            "max_btc": [150000000, 120000000],
            "btcvalue": [120000000, 100000000],
            "count_out": [5, 3],
            "first_in": ["2020-01-01", "2020-02-01"]
        })
        
        mock_delta_controller.get_pnl_stats.return_value = mock_pnl_stats
        
        result = crypto_analytics.get_top_addresses_by_profit("2023-10-01")
        
        # Verify the result is a list
        assert isinstance(result, list)
        assert len(result) == 2
        assert "bc1qaddress1" in result
        assert "bc1qaddress2" in result
        
        # Verify controller was called
        mock_delta_controller.get_pnl_stats.assert_called_once_with("2023-10-01")
    
    def test_get_top_addresses_by_profit_with_filters(self, crypto_analytics, mock_delta_controller):
        """Test getting top addresses with filters."""
        mock_pnl_stats = pl.DataFrame({
            "date_": ["2023-10-01", "2023-10-01"],
            "address": ["bc1qaddress1", "bc1qaddress2"],
            "profit2btc": [50000.0, 45000.0],
            "max_btc": [150000000, 120000000],
            "btcvalue": [120000000, 100000000],
            "count_out": [5, 3],
            "first_in": ["2020-01-01", "2020-02-01"]
        })
        
        mock_delta_controller.get_pnl_stats.return_value = mock_pnl_stats
        
        filters = {
            "profit2btc_min": 46000.0,
            "count_out_min": 4
        }
        
        result = crypto_analytics.get_top_addresses_by_profit("2023-10-01", filters)
        
        # Verify the result is filtered
        assert isinstance(result, list)
        # Only bc1qaddress1 should pass the filters
        assert len(result) == 1
        assert "bc1qaddress1" in result
    
    def test_get_top_addresses_by_profit_empty_result(self, crypto_analytics, mock_delta_controller):
        """Test getting top addresses when no results match."""
        mock_pnl_stats = pl.DataFrame({
            "date_": [],
            "address": [],
            "profit2btc": [],
            "max_btc": [],
            "btcvalue": [],
            "count_out": [],
            "first_in": []
        })
        
        mock_delta_controller.get_pnl_stats.return_value = mock_pnl_stats
        
        result = crypto_analytics.get_top_addresses_by_profit("2023-10-01")
        
        # Verify the result is an empty list
        assert isinstance(result, list)
        assert len(result) == 0
