"""
Script to generate expected results from mock data for testing.
Run this script to create/update expected result files.
"""
import asyncio
import json
import polars as pl
from unittest.mock import Mock
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.services.crypto_analytics import CryptoAnalytics
from core.db.delta_controller import DeltaTableController
from tests.fixtures.sample_data import (
    TEST_ADDRESS,
    get_sample_btc_quotes_dataframe,
    get_sample_input_transactions,
    get_sample_output_transactions
)


def create_mock_db_controller():
    """Create a mock SQLAlchemy controller."""
    controller = Mock()
    async def mock_get_btc_quotes():
        return get_sample_btc_quotes_dataframe()
    controller.get_btc_quotes = mock_get_btc_quotes
    return controller


def create_mock_delta_controller():
    """Create a mock Delta Table controller."""
    controller = Mock(spec=DeltaTableController)
    controller.get_input_transactions.return_value = get_sample_input_transactions()
    controller.get_output_transactions.return_value = get_sample_output_transactions()
    return controller


def save_result_to_file(result: dict, filename: str):
    """Save result to JSON file."""
    # Convert Polars DataFrame to dict for JSON serialization
    result_copy = result.copy()
    if 'balance_history' in result_copy:
        result_copy['balance_history'] = result_copy['balance_history'].to_dict(as_series=False)
    
    with open(filename, 'w') as f:
        json.dump(result_copy, f, indent=2, default=str)
    print(f"Saved result to {filename}")


def main():
    """Generate expected results for all test cases."""
    print("Generating expected results from mock data...")
    
    # Create controllers
    db_controller = create_mock_db_controller()
    delta_controller = create_mock_delta_controller()
    
    # Create CryptoAnalytics service
    crypto_analytics = CryptoAnalytics(db_controller, delta_controller)
    
    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), 'expected_results')
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate result for get_address_transactions
    print("\nGenerating result for get_address_transactions...")
    try:
        result = crypto_analytics.get_address_transactions(TEST_ADDRESS)
        save_result_to_file(result, os.path.join(output_dir, 'address_transactions_result.json'))
    except Exception as e:
        print(f"Error: {e}")
    
    # Generate result for get_address_balance
    print("\nGenerating result for get_address_balance...")
    try:
        result = crypto_analytics.get_address_balance(TEST_ADDRESS)
        # Convert DataFrame to dict for saving
        balance_dict = result.to_dict(as_series=False)
        with open(os.path.join(output_dir, 'address_balance_result.json'), 'w') as f:
            json.dump(balance_dict, f, indent=2, default=str)
        print(f"Saved result to address_balance_result.json")
    except Exception as e:
        print(f"Error: {e}")
    
    # Generate result for get_benchmark_metrics
    print("\nGenerating result for get_benchmark_metrics...")
    try:
        result = crypto_analytics.get_benchmark_metrics()
        benchmark_dict = {
            'sharpe': result.sharpe,
            'drawdown': result.drawdown,
            'profit_pct': result.profit_pct
        }
        with open(os.path.join(output_dir, 'benchmark_metrics_result.json'), 'w') as f:
            json.dump(benchmark_dict, f, indent=2, default=str)
        print(f"Saved result to benchmark_metrics_result.json")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\nAll expected results generated successfully!")


if __name__ == '__main__':
    main()
