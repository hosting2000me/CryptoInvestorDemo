"""
Sample data for unit tests.
"""
import polars as pl
import pandas as pd
from datetime import date, datetime


# Sample Bitcoin quotes data
SAMPLE_BTC_QUOTES = pl.DataFrame({
    "date_": [
        date(2020, 1, 1),
        date(2020, 1, 2),
        date(2020, 1, 3),
        date(2020, 1, 4),
        date(2020, 1, 5),
    ],
    "close_": [7000.0, 7100.0, 7200.0, 7150.0, 7300.0]
})


# Sample input transactions (after rename from exit_usdvalue to t_usdvalue)
SAMPLE_INPUT_TRANSACTIONS = pl.DataFrame({
    "t_time": [
        datetime(2020, 1, 2, 10, 0, 0),
        datetime(2020, 1, 3, 11, 0, 0),
    ],
    "address": ["bc1qtestaddress123", "bc1qtestaddress123"],
    "t_value": [100000000, 50000000],  # 1 BTC and 0.5 BTC in satoshis
    "t_usdvalue": [7100.0, 3600.0]
})


# Sample output transactions
SAMPLE_OUTPUT_TRANSACTIONS = pl.DataFrame({
    "t_time": [
        datetime(2020, 1, 4, 12, 0, 0),
        datetime(2020, 1, 5, 13, 0, 0),
    ],
    "address": ["bc1qtestaddress123", "bc1qtestaddress123"],
    "t_value": [30000000, 20000000],  # 0.3 BTC and 0.2 BTC in satoshis
    "t_usdvalue": [2145.0, 1460.0]
})


# Sample PNL stats
SAMPLE_PNL_STATS = pl.DataFrame({
    "date_": [date(2023, 10, 1), date(2023, 10, 1)],
    "address": ["bc1qtestaddress123", "bc1qtestaddress456"],
    "profit2btc": [50000.0, 45000.0],
    "max_btc": [150000000, 120000000],
    "btcvalue": [120000000, 100000000],
    "count_out": [5, 3],
    "first_in": [datetime(2020, 1, 1), datetime(2020, 2, 1)]
})


# PostgreSQL connection parameters for testing
TEST_POSTGRES_PARAMS = {
    "database": "test_btc",
    "user": "test_user",
    "password": "test_pass",
    "host": "localhost",
    "port": 5432
}


# Delta table paths for testing
TEST_TABLE_PATHS = {
    "inputs": "/test/data/btc/views/inputs_address",
    "outputs": "/test/data/btc/views/outputs_address",
    "pnl_stats": "/test/data/btc/pnl_stats"
}


# Sample address for testing
TEST_ADDRESS = "bc1qtestaddress123"


def get_sample_btc_quotes_dataframe() -> pl.DataFrame:
    """Get sample Bitcoin quotes as Polars DataFrame."""
    return SAMPLE_BTC_QUOTES.clone()


def get_sample_input_transactions() -> pl.DataFrame:
    """Get sample input transactions as Polars DataFrame."""
    return SAMPLE_INPUT_TRANSACTIONS.clone()


def get_sample_output_transactions() -> pl.DataFrame:
    """Get sample output transactions as Polars DataFrame."""
    return SAMPLE_OUTPUT_TRANSACTIONS.clone()


def get_sample_pnl_stats() -> pl.DataFrame:
    """Get sample PNL stats as Polars DataFrame."""
    return SAMPLE_PNL_STATS.clone()
