"""
Unit tests for Delta Table Controller.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import polars as pl

from core.db.delta_controller import DeltaTableController
from tests.fixtures.sample_data import (
    TEST_TABLE_PATHS,
    TEST_ADDRESS,
    get_sample_input_transactions,
    get_sample_output_transactions,
    get_sample_pnl_stats
)


@pytest.fixture
def delta_controller():
    """Create a Delta Table controller instance."""
    return DeltaTableController(TEST_TABLE_PATHS)


class TestDeltaTableController:
    """Test suite for DeltaTableController."""
    
    def test_init(self):
        """Test controller initialization."""
        controller = DeltaTableController(TEST_TABLE_PATHS)
        assert controller.table_paths == TEST_TABLE_PATHS
    
    def test_calculate_partition(self, delta_controller):
        """Test partition calculation."""
        address = "bc1qtestaddress123"
        partition = delta_controller._calculate_partition(address)
        
        # Partition should be between 0 and 999
        assert 0 <= partition < 1000
        assert isinstance(partition, int)
    
    def test_calculate_partition_deterministic(self, delta_controller):
        """Test that partition calculation is deterministic."""
        address = "bc1qtestaddress123"
        partition1 = delta_controller._calculate_partition(address)
        partition2 = delta_controller._calculate_partition(address)
        
        assert partition1 == partition2
    
    def test_calculate_partition_different_addresses(self, delta_controller):
        """Test that different addresses produce different partitions (usually)."""
        address1 = "bc1qtestaddress123"
        address2 = "bc1qtestaddress456"
        
        partition1 = delta_controller._calculate_partition(address1)
        partition2 = delta_controller._calculate_partition(address2)
        
        # They might be the same by chance, but usually different
        # Just verify both are valid
        assert 0 <= partition1 < 1000
        assert 0 <= partition2 < 1000
    
    @patch('core.db.delta_controller.pl.scan_delta')
    def test_get_input_transactions(self, mock_scan_delta, delta_controller):
        """Test retrieving input transactions."""
        # Mock the lazy frame and collect
        mock_lazy_frame = Mock()
        mock_result_frame = get_sample_input_transactions()
        
        # Set up the chain: scan_delta -> filter -> select -> rename -> collect
        mock_scan_delta.return_value = mock_lazy_frame
        mock_lazy_frame.filter.return_value = mock_lazy_frame
        mock_lazy_frame.select.return_value = mock_lazy_frame
        mock_lazy_frame.rename.return_value = mock_lazy_frame
        mock_lazy_frame.collect.return_value = mock_result_frame
        
        result = delta_controller.get_input_transactions(TEST_ADDRESS)
        
        # Verify the result
        assert isinstance(result, pl.DataFrame)
        assert "t_time" in result.columns
        assert "address" in result.columns
        assert "t_value" in result.columns
        assert "t_usdvalue" in result.columns
        
        # Verify scan_delta was called with correct parameters
        mock_scan_delta.assert_called_once()
        call_args = mock_scan_delta.call_args
        assert call_args[0][0] == TEST_TABLE_PATHS["inputs"]
    
    @patch('core.db.delta_controller.pl.scan_delta')
    def test_get_output_transactions(self, mock_scan_delta, delta_controller):
        """Test retrieving output transactions."""
        mock_lazy_frame = Mock()
        mock_result_frame = get_sample_output_transactions()
        
        mock_scan_delta.return_value = mock_lazy_frame
        mock_lazy_frame.filter.return_value = mock_lazy_frame
        mock_lazy_frame.select.return_value = mock_lazy_frame
        mock_lazy_frame.collect.return_value = mock_result_frame
        
        result = delta_controller.get_output_transactions(TEST_ADDRESS)
        
        # Verify the result
        assert isinstance(result, pl.DataFrame)
        assert "t_time" in result.columns
        assert "address" in result.columns
        assert "t_value" in result.columns
        assert "t_usdvalue" in result.columns
        
        # Verify scan_delta was called with correct parameters
        mock_scan_delta.assert_called_once()
        call_args = mock_scan_delta.call_args
        assert call_args[0][0] == TEST_TABLE_PATHS["outputs"]
    
    @patch('core.db.delta_controller.pl.scan_delta')
    def test_get_pnl_stats(self, mock_scan_delta, delta_controller):
        """Test retrieving PNL statistics."""
        mock_lazy_frame = Mock()
        mock_result_frame = get_sample_pnl_stats()
        
        mock_scan_delta.return_value = mock_lazy_frame
        mock_lazy_frame.filter.return_value = mock_lazy_frame
        mock_lazy_frame.with_columns.return_value = mock_lazy_frame
        mock_lazy_frame.collect.return_value = mock_result_frame
        
        test_date = "2023-10-01"
        result = delta_controller.get_pnl_stats(test_date)
        
        # Verify the result
        assert isinstance(result, pl.DataFrame)
        assert "date_" in result.columns
        assert "address" in result.columns
        
        # Verify scan_delta was called with correct parameters
        mock_scan_delta.assert_called_once()
        call_args = mock_scan_delta.call_args
        assert call_args[0][0] == TEST_TABLE_PATHS["pnl_stats"]
    
    @patch('core.db.delta_controller.pl.scan_delta')
    def test_get_all_transactions(self, mock_scan_delta, delta_controller):
        """Test retrieving all transactions (input + output)."""
        mock_lazy_frame = Mock()
        mock_input_frame = get_sample_input_transactions()
        mock_output_frame = get_sample_output_transactions()
        
        # First call for input, second for output
        mock_scan_delta.return_value = mock_lazy_frame
        mock_lazy_frame.filter.return_value = mock_lazy_frame
        mock_lazy_frame.select.return_value = mock_lazy_frame
        mock_lazy_frame.rename.return_value = mock_lazy_frame
        
        # Return different results for each call
        call_count = [0]
        
        def collect_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return mock_input_frame
            else:
                return mock_output_frame
        
        mock_lazy_frame.collect.side_effect = collect_side_effect
        
        result = delta_controller.get_all_transactions(TEST_ADDRESS)
        
        # Verify the result combines both
        assert isinstance(result, pl.DataFrame)
        assert len(result) == len(mock_input_frame) + len(mock_output_frame)
        
        # Verify scan_delta was called twice
        assert mock_scan_delta.call_count == 2
    
    def test_get_input_transactions_missing_path(self, delta_controller):
        """Test error handling when inputs path is not configured."""
        controller = DeltaTableController({"inputs": "", "outputs": TEST_TABLE_PATHS["outputs"]})
        
        with pytest.raises(ValueError, match="Inputs table path not configured"):
            controller.get_input_transactions(TEST_ADDRESS)
    
    def test_get_output_transactions_missing_path(self, delta_controller):
        """Test error handling when outputs path is not configured."""
        controller = DeltaTableController({"inputs": TEST_TABLE_PATHS["inputs"], "outputs": ""})
        
        with pytest.raises(ValueError, match="Outputs table path not configured"):
            controller.get_output_transactions(TEST_ADDRESS)
    
    def test_get_pnl_stats_missing_path(self, delta_controller):
        """Test error handling when pnl_stats path is not configured."""
        controller = DeltaTableController({
            "inputs": TEST_TABLE_PATHS["inputs"],
            "outputs": TEST_TABLE_PATHS["outputs"],
            "pnl_stats": ""
        })
        
        with pytest.raises(ValueError, match="PNL stats table path not configured"):
            controller.get_pnl_stats("2023-10-01")
