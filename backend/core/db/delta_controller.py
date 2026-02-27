"""
Delta Table Controller for Bitcoin address transactions.
"""
import zlib
import polars as pl
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class DeltaTableController:
    """Controller for Delta table operations."""
    
    def __init__(self, table_paths: Dict[str, str]):
        """
        Initialize the Delta Table controller.
        
        Args:
            table_paths: Dictionary containing Delta table paths:
                - inputs: Path to inputs address view
                - outputs: Path to outputs address view
                - pnl_stats: Path to PNL statistics table
        """
        self.table_paths = table_paths
        
    def _calculate_partition(self, address: str) -> int:
        """
        Calculate partition number for an address using CRC32.
        
        Args:
            address: Bitcoin address
            
        Returns:
            Partition number (0-999)
        """
        return zlib.crc32(address.encode('utf-8')) % 1000
    
    def get_input_transactions(self, address: str) -> pl.DataFrame:
        """
        Retrieve input transactions for a specific Bitcoin address.
        
        Args:
            address: Bitcoin address
            
        Returns:
            Polars DataFrame with columns: t_time, address, t_value, t_usdvalue
        """
        table_path = self.table_paths.get("inputs")
        if not table_path:
            raise ValueError("Inputs table path not configured")
        
        partition = self._calculate_partition(address)
        
        try:
            df = pl.scan_delta(
                table_path,
                use_pyarrow=True,
                pyarrow_options={
                    "partitions": [("partition_", "=", str(partition))]
                }
            )
            
            df = df.filter(pl.col('address') == address)
            df = df.select('t_time', 'address', 't_value', 'exit_usdvalue')
            df = df.rename({"exit_usdvalue": "t_usdvalue"})
            
            result = df.collect()
            logger.info(f"Retrieved {len(result)} input transactions for address {address}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to retrieve input transactions for address {address}: {e}")
            raise
    
    def get_output_transactions(self, address: str) -> pl.DataFrame:
        """
        Retrieve output transactions for a specific Bitcoin address.
        
        Args:
            address: Bitcoin address
            
        Returns:
            Polars DataFrame with columns: t_time, address, t_value, t_usdvalue
        """
        table_path = self.table_paths.get("outputs")
        if not table_path:
            raise ValueError("Outputs table path not configured")
        
        partition = self._calculate_partition(address)
        
        try:
            df = pl.scan_delta(
                table_path,
                use_pyarrow=True,
                pyarrow_options={
                    "partitions": [("partition_", "=", str(partition))]
                }
            )
            
            df = df.filter(pl.col('address') == address)
            df = df.select('t_time', 'address', 't_value', 't_usdvalue')
            
            result = df.collect()
            logger.info(f"Retrieved {len(result)} output transactions for address {address}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to retrieve output transactions for address {address}: {e}")
            raise
    
    def get_pnl_stats(self, date: str) -> pl.DataFrame:
        """
        Retrieve PNL statistics for a specific date.
        
        Args:
            date: Date string in format 'YYYY-MM-DD'
            
        Returns:
            Polars DataFrame with PNL statistics
        """
        table_path = self.table_paths.get("pnl_stats")
        if not table_path:
            raise ValueError("PNL stats table path not configured")
        
        try:
            df = pl.scan_delta(table_path).filter(pl.col('date_') == date)
            df = df.with_columns([
                pl.col("btcvalue").cast(pl.Int64),
                pl.col("max_btc").cast(pl.Int64)
            ])
            
            result = df.collect()
            logger.info(f"Retrieved {len(result)} PNL stats for date {date}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to retrieve PNL stats for date {date}: {e}")
            raise
    
    def get_all_transactions(self, address: str) -> pl.DataFrame:
        """
        Retrieve both input and output transactions for a specific Bitcoin address.
        
        Args:
            address: Bitcoin address
            
        Returns:
            Polars DataFrame with combined transactions
        """
        df_in = self.get_input_transactions(address)
        df_out = self.get_output_transactions(address)
        
        # Combine both dataframes
        df_combined = pl.concat([df_in, df_out])
        
        logger.info(f"Retrieved {len(df_combined)} total transactions for address {address}")
        return df_combined
