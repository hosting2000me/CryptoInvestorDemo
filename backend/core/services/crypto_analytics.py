"""
Crypto Analytics Service for Bitcoin address analysis.
"""
import polars as pl
import pandas as pd
import numpy as np
import empyrical as strata
from typing import Dict, Optional, Tuple, TYPE_CHECKING
from datetime import datetime
import logging
import asyncio

if TYPE_CHECKING:
    from ..db.sqlalchemy_controller import SQLAlchemyController

from ..db import DeltaTableController
from ..models.schemas import AddressStats, BenchmarkMetrics

logger = logging.getLogger(__name__)


class CryptoAnalytics:
    """Service for analyzing Bitcoin addresses and calculating financial metrics."""
    
    def __init__(self, db_controller: 'SQLAlchemyController', delta_controller: DeltaTableController):
        """
        Initialize the CryptoAnalytics service.
        
        Args:
            db_controller: Controller for database operations using SQLAlchemy
            delta_controller: Controller for Delta table operations
        """
        self.db_controller = db_controller
        self.delta_controller = delta_controller
    
    def _calculate_benchmark(self, btc_quotes: pl.DataFrame) -> BenchmarkMetrics:
        """
        Calculate benchmark metrics (buy and hold strategy).
        
        Args:
            btc_quotes: Polars DataFrame with Bitcoin price quotes
            
        Returns:
            BenchmarkMetrics object with sharpe, drawdown, and profit_pct
        """
        df = btc_quotes.with_columns(pl.col("close_").pct_change().alias("returns")).to_pandas()
        
        sharpe = strata.sharpe_ratio(df.returns, risk_free=0, period='daily')
        drawdown = strata.max_drawdown(df.returns)
        profit_pct = (df['close_'].iloc[-1] / df['close_'].iloc[0] - 1)
        
        return BenchmarkMetrics(sharpe=sharpe, drawdown=drawdown, profit_pct=profit_pct)
    
    def get_address_transactions(self, btc_address: str) -> Dict:
        """
        Calculate transaction statistics for a Bitcoin address.
        
        Args:
            btc_address: Bitcoin address to analyze
            
        Returns:
            Dictionary containing address statistics and balance history
        """
        # Retrieve transaction data
        trans_in = self.delta_controller.get_input_transactions(btc_address)
        trans_out = self.delta_controller.get_output_transactions(btc_address)
        
        # Retrieve Bitcoin quotes (async call wrapped in asyncio.run)
        btc_quotes = asyncio.run(self.db_controller.get_btc_quotes())
        
        # Filter transactions for the specific address
        trans_out = trans_out.filter(pl.col('address') == btc_address)
        trans_in = trans_in.filter(pl.col('address') == btc_address)
        trans_out = trans_out.sort('t_time', descending=False)
        
        # Convert timestamps to dates
        trans_in = trans_in.with_columns(pl.col('t_time').cast(pl.Date).alias('date_'))
        trans_out = trans_out.with_columns(pl.col('t_time').cast(pl.Date).alias('date_'))
        
        # Get first output date and filter quotes
        if len(trans_out) == 0:
            raise ValueError(f"No output transactions found for address {btc_address}")
        
        first_output = trans_out.select(pl.col('date_')).row(0)[0]
        btc_quotes = btc_quotes.filter(pl.col('date_') >= first_output)
        
        # Union IN and OUT transactions
        # Input transactions: negative t_value (giving away BTC)
        # Output transactions: negative t_usdvalue (receiving USD)
        df1 = trans_in.with_columns(pl.col("t_value") * (-1))
        df2 = trans_out.with_columns(pl.col("t_usdvalue") * (-1))
        trans_all = pl.concat([df1, df2])
        
        # Sort by date and group by date
        trans_all = trans_all.sort('t_time', descending=False)
        trans_all = trans_all.group_by("date_").agg(
            pl.col("t_value").sum(),
            pl.col("t_usdvalue").sum()
        )
        
        # Join with Bitcoin prices
        df = btc_quotes.join(trans_all, on='date_', how='left')
        df = df.fill_null(0)
        
        # Calculate cumulative sums
        df = df.with_columns(pl.col("t_value").cum_sum().name.suffix("_cumsum"))
        df = df.with_columns(pl.col("t_usdvalue").cum_sum().name.suffix("_cumsum"))
        
        # Protection against selling BTC we don't have
        df = df.with_columns(
            pl.when(pl.col("t_value_cumsum") >= 0)
            .then(pl.col("t_usdvalue"))
            .otherwise(0)
            .alias('t_usdvalue')
        )
        df = df.with_columns(
            pl.when(pl.col("t_value_cumsum") >= 0)
            .then(pl.col("t_value"))
            .otherwise(0)
            .alias('t_value')
        )
        
        # Recalculate cumulative sums after protection
        df = df.with_columns(pl.col("t_value").cum_sum().name.suffix("_cumsum"))
        df = df.with_columns(pl.col("t_usdvalue").cum_sum().name.suffix("_cumsum"))
        
        # Calculate margin (delta in USD)
        df = df.with_columns(
            (pl.col('t_usdvalue_cumsum') + pl.col('t_value_cumsum') * pl.col('close_') / 100000000)
            .alias('delta_usd')
        )
        
        # Calculate returns
        initial_value = abs(df.select(pl.col("t_usdvalue_cumsum")).min().item(0, 0))
        max_btc_value = df.select(pl.col("t_value_cumsum").abs()).max().item(0, 0)
        
        df = df.with_columns((pl.col("delta_usd") + initial_value).pct_change().alias("returns"))
        df = df.with_columns(pl.col('returns').log1p().alias("log_returns"))
        
        # Calculate exposure
        df = df.with_columns((pl.col("t_value_cumsum") / max_btc_value).alias("exposure"))
        exposure = df['exposure'].mean()
        
        # Calculate profit percentage
        profit_pct = df["log_returns"].sum()
        profit_pct = np.exp(profit_pct) - 1
        
        df = df.fill_nan(0)
        
        # Count days in market with significant BTC holdings
        count_days_in_market = len(df.filter(pl.col('t_value_cumsum') > 100000000))
        
        # Convert to pandas for empyrical calculations
        df_pandas = df.with_columns(pl.col('delta_usd').alias('value')).to_pandas()
        
        # Calculate Sharpe ratio and drawdown
        sharp_ratio = strata.sharpe_ratio(df_pandas.returns, risk_free=0, period='daily')
        drawdown = strata.max_drawdown(df_pandas.returns)
        
        # Calculate benchmark metrics
        benchmark = self._calculate_benchmark(btc_quotes)
        
        logger.info(f"Calculated metrics for address {btc_address}: "
                   f"profit={profit_pct:.2%}, sharpe={sharp_ratio:.2f}, drawdown={drawdown:.2%}")
        
        return {
            "address": btc_address,
            "profit_pct": float(profit_pct),
            "sharpe_ratio": float(sharp_ratio),
            "drawdown": float(drawdown),
            "exposure": float(exposure),
            "count_days_in_market": int(count_days_in_market),
            "benchmark_profit": float(benchmark.profit_pct),
            "benchmark_sharpe": float(benchmark.sharpe),
            "benchmark_drawdown": float(benchmark.drawdown),
            "balance_history": df
        }
    
    def get_address_transactions2(btc_addr, trans_in, trans_out, btc_prices):
        trans_out = trans_out.filter(pl.col('address') == btc_addr)
        trans_in = trans_in.filter(pl.col('address') == btc_addr)
        trans_out = trans_out.sort('t_time', descending = False)
        # конвертируем время в дату
        trans_in = trans_in.with_columns(pl.col('t_time').cast(pl.Date).alias('date_'))
        trans_out = trans_out.with_columns(pl.col('t_time').cast(pl.Date).alias('date_'))
        first_output = trans_out.select(pl.col('date_')).row(0)[0]
        btc_prices = btc_prices.filter(pl.col('date_') >= first_output)
        # UNION IN and OUT trans (in_transactions with minus  
        # мы берем со знаком минус t_value из inputs и t_usdvalue из outputs, так как отдаем битки и получаем баксы
        df1 = trans_in.with_columns(pl.col("t_value") * (-1))
        df2 = trans_out.with_columns(pl.col("t_usdvalue") * (-1))
        trans_all = pl.concat([df1, df2]) 
        # Сортировка по возрастанию дат
        trans_all = trans_all.sort('t_time', descending = False)
        # группировка по датам для этого кошелька
        #trans_all = trans_all.with_columns(pl.col('t_time').cast(pl.Date).alias('date_'))
        trans_all = trans_all.group_by("date_").agg(pl.col("t_value").sum(), pl.col("t_usdvalue").sum()) 
        # Объединяем с ценами на биток
        df = btc_prices.join(trans_all , on='date_', how='left')
        #df = df.with_columns(pl.col('t_value').fill_null(0))
        df = df.fill_null(0)
        # добавляем нарастающий итог t_value & t_usdvalue на каждую дату, поле _cumsum
        df = df.with_columns(pl.col("t_value").cum_sum().name.suffix("_cumsum"),)  
        df = df.with_columns(pl.col("t_usdvalue").cum_sum().name.suffix("_cumsum"),) 
        # Защита от того что мы продаем битки которых у нас нет
        # Если мы ушли в минус по биткам то обнуляем эту транзакцию и потом считаем нарастающий итог по новой
        df = df.with_columns(pl.when(pl.col("t_value_cumsum") >= 0).then(pl.col("t_usdvalue")).otherwise(0).alias('t_usdvalue'))
        df = df.with_columns(pl.when(pl.col("t_value_cumsum") >= 0).then(pl.col("t_value")).otherwise(0).alias('t_value'))
        df = df.with_columns(pl.col("t_value").cum_sum().name.suffix("_cumsum"),)  
        df = df.with_columns(pl.col("t_usdvalue").cum_sum().name.suffix("_cumsum"),) 
        # считаем маржу, это уже будет нарастающий итог(!!!)
        df = df.with_columns(  (pl.col('t_usdvalue_cumsum') + pl.col('t_value_cumsum')*pl.col('close_')/100000000).alias('delta_usd')  )    
        # считаем ежедневный return в процентах (тело считаем как t_value_cumsum*btc_price) либо как max(t_value_cumsum)
        # initial_value как максимальный приход денег в рынок, когда покупаем btc то t_usdvalue со знаком минус
        initial_value = abs(df.select(pl.col("t_usdvalue_cumsum")).min().item(0,0))
        max_btc_value = df.select(pl.col("t_value_cumsum").abs()).max().item(0,0)
        df = df.with_columns( (pl.col("delta_usd") + initial_value).pct_change().alias("returns"))
        df = df.with_columns( pl.col('returns').log1p().alias("log_returns")) # переводим в logNormal returns (natural logarithm)
        #df = df.with_columns(pl.col("returns").cum_sum().name.suffix("_cumsum"),) 
        #df = df.with_columns(pl.col("returns_cumsum").exp())
        df = df.with_columns( (pl.col("t_value_cumsum") / max_btc_value).alias("exposure"))
        exposure = df['exposure'].mean()
        profit_pct = df["log_returns"].sum()
        profit_pct = np.exp(profit_pct) - 1
        df = df.fill_nan(0)
        # вычисляем количество дней в рынке с битком на руках
        count_days_in_market = len(df.filter(pl.col('t_value_cumsum') > 100000000))
        df = df.with_columns(pl.col('delta_usd').alias('value')).to_pandas()
        #df = df.with_columns(pl.col('returns_cumsum').alias('value')).to_pandas()
        #df = df.fillna(0)
        sharp_ratio = strata.sharpe_ratio(df.returns, risk_free=0, period='daily')
        drawdown = strata.max_drawdown(df.returns)
        benchmark_sharpe, benchmark_drawdown = benchmark(btc_prices) 
        #print(benchmark_sharp, benchmark_drawdown)
        # добавляем нарастающий итог стоимости портфеля на каждую дату, поле _cumsum
        #trans_all = trans_all.with_columns(pl.col("t_usdvalue").cum_sum().name.suffix("_cumsum"),)
        #trans_all.sort('date_', descending = False)
        return df, sharp_ratio, drawdown, exposure, benchmark_sharpe, benchmark_drawdown, count_days_in_market, profit_pct
    
    def get_address_balance(self, btc_address: str) -> pl.DataFrame:
        """
        Calculate balance history for a Bitcoin address.
        
        Args:
            btc_address: Bitcoin address to analyze
            
        Returns:
            Polars DataFrame with balance history
        """
        # Retrieve transaction data
        trans_in = self.delta_controller.get_input_transactions(btc_address)
        trans_out = self.delta_controller.get_output_transactions(btc_address)
        
        # Retrieve Bitcoin quotes (async call wrapped in asyncio.run)
        btc_quotes = asyncio.run(self.db_controller.get_btc_quotes())
        
        # Filter transactions for the specific address
        trans_out = trans_out.filter(pl.col('address') == btc_address)
        trans_in = trans_in.filter(pl.col('address') == btc_address)
        trans_out = trans_out.sort('t_time', descending=False)
        
        # Convert timestamps to dates
        trans_in = trans_in.with_columns(pl.col('t_time').cast(pl.Date).alias('date_'))
        trans_out = trans_out.with_columns(pl.col('t_time').cast(pl.Date).alias('date_'))
        
        # Get first output date and filter quotes
        if len(trans_out) == 0:
            raise ValueError(f"No output transactions found for address {btc_address}")
        
        first_output = trans_out.select(pl.col('date_')).row(0)[0]
        btc_quotes = btc_quotes.filter(pl.col('date_') >= first_output)
        
        # Union IN and OUT transactions
        df1 = trans_in.with_columns(pl.col("t_value") * (-1))
        df2 = trans_out.with_columns(pl.col("t_usdvalue") * (-1))
        trans_all = pl.concat([df1, df2])
        
        # Sort by date and group by date
        trans_all = trans_all.sort('t_time', descending=False)
        trans_all = trans_all.group_by("date_").agg(
            pl.col("t_value").sum(),
            pl.col("t_usdvalue").sum()
        )
        
        # Join with Bitcoin prices
        df = btc_quotes.join(trans_all, on='date_', how='left')
        df = df.fill_null(0)
        
        # Calculate cumulative sums
        df = df.with_columns(pl.col("t_value").cum_sum().name.suffix("_cumsum"))
        df = df.with_columns(pl.col("t_usdvalue").cum_sum().name.suffix("_cumsum"))
        
        # Protection against selling BTC we don't have
        df = df.with_columns(
            pl.when(pl.col("t_value_cumsum") >= 0)
            .then(pl.col("t_usdvalue"))
            .otherwise(0)
            .alias('t_usdvalue')
        )
        df = df.with_columns(
            pl.when(pl.col("t_value_cumsum") >= 0)
            .then(pl.col("t_value"))
            .otherwise(0)
            .alias('t_value')
        )
        
        # Recalculate cumulative sums after protection
        df = df.with_columns(pl.col("t_value").cum_sum().name.suffix("_cumsum"))
        df = df.with_columns(pl.col("t_usdvalue").cum_sum().name.suffix("_cumsum"))
        
        # Return BTC balance as value
        df = df.with_columns(pl.col('t_value_cumsum').alias('value'))
        
        logger.info(f"Calculated balance history for address {btc_address}")
        return df
    
    def get_benchmark_metrics(self) -> BenchmarkMetrics:
        """
        Get benchmark metrics for Bitcoin (buy and hold strategy).
        
        Returns:
            BenchmarkMetrics object
        """
        btc_quotes = asyncio.run(self.db_controller.get_btc_quotes())
        return self._calculate_benchmark(btc_quotes)
    
    def get_top_addresses_by_profit(self, date: str, filters: Optional[Dict] = None) -> list:
        """
        Get top addresses by profit for a specific date.
        
        Args:
            date: Date string in format 'YYYY-MM-DD'
            filters: Optional dictionary of filters to apply
            
        Returns:
            List of addresses sorted by profit
        """
        df_pnl = self.delta_controller.get_pnl_stats(date)
        
        if filters:
            # Apply filters
            if 'profit2btc_min' in filters:
                df_pnl = df_pnl.filter(pl.col('profit2btc') > filters['profit2btc_min'])
            if 'max_btc_min' in filters:
                df_pnl = df_pnl.filter(pl.col('max_btc') > filters['max_btc_min'])
            if 'btcvalue_ratio_min' in filters:
                df_pnl = df_pnl.filter(
                    pl.col('btcvalue') > pl.col('max_btc') * filters['btcvalue_ratio_min']
                )
            if 'count_out_min' in filters:
                df_pnl = df_pnl.filter(pl.col('count_out') >= filters['count_out_min'])
            if 'first_in_after' in filters:
                df_pnl = df_pnl.filter(
                    pl.col('first_in').cast(pl.Date) > filters['first_in_after']
                )
        
        # Sort by profit and return addresses
        addresses = df_pnl.sort('profit2btc', descending=True)
        address_list = addresses['address'].to_list()
        
        logger.info(f"Found {len(address_list)} addresses for date {date}")
        return address_list
