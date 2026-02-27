"""
Pydantic models and schemas for data validation.
"""
from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel, Field


class BitcoinQuote(BaseModel):
    """Schema for Bitcoin price quote data."""
    date_: date = Field(..., description="Date of the quote")
    close_: float = Field(..., description="Closing price in USD")


class Transaction(BaseModel):
    """Schema for Bitcoin transaction data."""
    t_time: datetime = Field(..., description="Transaction timestamp")
    address: str = Field(..., description="Bitcoin address")
    t_value: int = Field(..., description="Transaction value in satoshis")
    t_usdvalue: Optional[float] = Field(None, description="Transaction value in USD")


class AddressStats(BaseModel):
    """Schema for Bitcoin address statistics."""
    address: str = Field(..., description="Bitcoin address")
    profit_pct: float = Field(..., description="Profit percentage")
    sharpe_ratio: float = Field(..., description="Sharpe ratio")
    drawdown: float = Field(..., description="Maximum drawdown")
    exposure: float = Field(..., description="Average exposure to Bitcoin")
    count_days_in_market: int = Field(..., description="Number of days with Bitcoin holdings")
    benchmark_profit: float = Field(..., description="Benchmark (buy and hold) profit percentage")
    benchmark_sharpe: float = Field(..., description="Benchmark Sharpe ratio")
    benchmark_drawdown: float = Field(..., description="Benchmark maximum drawdown")


class BenchmarkMetrics(BaseModel):
    """Schema for benchmark metrics."""
    sharpe: float = Field(..., description="Sharpe ratio")
    drawdown: float = Field(..., description="Maximum drawdown")
    profit_pct: float = Field(..., description="Profit percentage")


class AddressFilter(BaseModel):
    """Schema for filtering addresses by criteria."""
    profit2btc_min: Optional[float] = Field(None, description="Minimum profit in BTC")
    max_btc_min: Optional[int] = Field(None, description="Minimum maximum BTC held")
    btcvalue_ratio_min: Optional[float] = Field(None, description="Minimum current BTC value ratio to max")
    count_out_min: Optional[int] = Field(None, description="Minimum number of output transactions")
    first_in_after: Optional[date] = Field(None, description="First input after this date")
