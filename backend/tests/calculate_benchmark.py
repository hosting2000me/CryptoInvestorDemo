"""
Calculate benchmark metrics from quotes.csv
"""
import polars as pl
import pandas as pd
import empyrical as strata
import json
from datetime import date

# Read quotes.csv
df = pl.read_csv("tests/fixtures/quotes.csv", try_parse_dates=True).with_columns(
    pl.col("date_").cast(pl.Date).alias("date_")
).sort("date_")

# Filter by first_output date (from sample transactions)
# First output date is 2020-06-30
first_output = date(2020, 6, 30)
df = df.filter(pl.col('date_') >= first_output)

# Calculate returns
df_pandas = df.with_columns(pl.col("close_").pct_change().alias("returns")).to_pandas()

# Calculate metrics
sharpe = strata.sharpe_ratio(df_pandas.returns, risk_free=0, period='daily')
drawdown = strata.max_drawdown(df_pandas.returns)
profit_pct = (df_pandas['close_'].iloc[-1] / df_pandas['close_'].iloc[0] - 1)

print(f"First date (filtered): {df_pandas['date_'].iloc[0]}")
print(f"Last date (filtered): {df_pandas['date_'].iloc[-1]}")
print(f"First close: {df_pandas['close_'].iloc[0]}")
print(f"Last close: {df_pandas['close_'].iloc[-1]}")
print(f"\nBenchmark metrics:")
print(f"sharpe: {sharpe}")
print(f"drawdown: {drawdown}")
print(f"profit_pct: {profit_pct}")

# Save to JSON
result = {
    "sharpe": sharpe,
    "drawdown": drawdown,
    "profit_pct": profit_pct
}

with open("tests/expected_results/benchmark_metrics_result.json", "w") as f:
    json.dump(result, f, indent=2)

print("\nSaved to benchmark_metrics_result.json")
