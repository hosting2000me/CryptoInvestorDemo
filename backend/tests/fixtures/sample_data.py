"""
Sample data for unit tests.
"""
import polars as pl
import pandas as pd
from datetime import date, datetime

# Sample Bitcoin quotes data - covers date range for all transactions (2020-2025)
# Note: Using quotes.csv instead of hardcoded data


# Sample input transactions (after rename from exit_usdvalue to t_usdvalue)
SAMPLE_INPUT_TRANSACTIONS = pl.DataFrame({
    "t_time": [
        datetime(2021, 6, 22, 22, 51, 19),
        datetime(2021, 6, 22, 22, 51, 19),
        datetime(2021, 1, 2, 20, 53, 32),
        datetime(2021, 1, 11, 21, 8, 38),
        datetime(2025, 3, 23, 21, 9, 28),
        datetime(2025, 3, 23, 21, 9, 28),
        datetime(2025, 3, 23, 21, 9, 28),
        datetime(2025, 4, 22, 22, 43, 52),
        datetime(2025, 4, 12, 21, 31, 4),
    ],
    "address": [
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
    ],
    "t_value": [26700000, 50000000, 300000000, 102000403, 330, 10000000, 600, 100980000, 16600000],  # in satoshis
    "t_usdvalue": [8576.3066, 16060.5, 88005.0, 38655.0938, 0.2767, 8385.9004, 0.5032, 88358.5078, 13842.0762]
})


# Sample output transactions
SAMPLE_OUTPUT_TRANSACTIONS = pl.DataFrame({
    "t_time": [
        datetime(2020, 6, 30, 21, 4, 57),
        datetime(2021, 5, 24, 21, 21, 34),
        datetime(2021, 1, 9, 23, 44, 23),
        datetime(2021, 6, 21, 21, 7, 35),
        datetime(2022, 12, 20, 23, 0, 49),
        datetime(2023, 6, 9, 22, 0, 44),
        datetime(2023, 8, 20, 21, 41, 53),
        datetime(2023, 10, 1, 9, 58, 12),
        datetime(2024, 8, 5, 21, 51, 49),
        datetime(2025, 1, 17, 23, 45, 4),
    ],
    "address": [
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
        "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf",
    ],
    "t_value": [300000000, 26700000, 102000403, 50000000, 100980000, 209990000, 16600000, 600, 10000000, 330],  # in satoshis
    "t_usdvalue": [27581.1289, 9397.5986, 41899.7266, 17890.5, 16603.8594, 55672.5469, 4331.77, 0.1619, 5823.7002, 0.3299]
})


# Sample PNL stats
SAMPLE_PNL_STATS = pl.DataFrame({
    "date_": [date(2023, 10, 1), date(2023, 10, 1)],
    "address": ["bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf", "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf"],
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
TEST_ADDRESS = "bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf"


def get_sample_btc_quotes_dataframe() -> pl.DataFrame:
    """Get sample Bitcoin quotes as Polars DataFrame."""
    return pl.read_csv("tests/fixtures/quotes.csv", try_parse_dates=True).with_columns(
        pl.col("date_").cast(pl.Date).alias("date_")
    ).sort("date_")


def get_sample_input_transactions() -> pl.DataFrame:
    """Get sample input transactions as Polars DataFrame."""
    return SAMPLE_INPUT_TRANSACTIONS.clone()


def get_sample_output_transactions() -> pl.DataFrame:
    """Get sample output transactions as Polars DataFrame."""
    return SAMPLE_OUTPUT_TRANSACTIONS.clone()


def get_sample_pnl_stats() -> pl.DataFrame:
    """Get sample PNL stats as Polars DataFrame."""
    return SAMPLE_PNL_STATS.clone()
