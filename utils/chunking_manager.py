import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

from conf.config import config

class ChunkingManager:
    """
    Centralized manager for calculating chunk sizes based on memory usage and load type.
    """
    
    # Load Config
    try:
        _config = config
        _chunk_config = _config.get("chunking", {})
    except Exception:
        _chunk_config = {}

    # Defaults from Config or Hardcoded Fallback
    DEFAULT_CHUNK_SIZE_SCD2_MB = _chunk_config.get("scd2_chunk_size_mb", 75)
    DEFAULT_CHUNK_SIZE_APPEND_MB = _chunk_config.get("append_chunk_size_mb", 200)
    DEFAULT_CHUNK_SIZE_TRUNCATE_MB = _chunk_config.get("truncate_chunk_size_mb", 200)
    
    # Safety Limits
    MIN_CHUNK_SIZE_MB = _chunk_config.get("min_chunk_size_mb", 25)
    MAX_CHUNK_SIZE_MB = _chunk_config.get("max_chunk_size_mb", 300)

    @staticmethod
    def resolve_rows_per_chunk(df: pd.DataFrame, load_type: str, explicit_chunk_mb: int = None) -> int:
        """
        Calculates the optimal number of rows per chunk.

        Args:
            df (pd.DataFrame): The DataFrame to be chunked.
            load_type (str): The type of load ('scd2', 'append', 'truncate').
            explicit_chunk_mb (int, optional): Explicit chunk size in MB.

        Returns:
            int: Number of rows per chunk.
        """
        if df.empty:
            return 1000  # Default safe value for empty frames

        # Determine target MB
        if explicit_chunk_mb:
            target_mb = explicit_chunk_mb
        elif load_type == 'scd2':
            target_mb = ChunkingManager.DEFAULT_CHUNK_SIZE_SCD2_MB
        elif load_type == 'truncate':
            target_mb = ChunkingManager.DEFAULT_CHUNK_SIZE_TRUNCATE_MB
        else:
            target_mb = ChunkingManager.DEFAULT_CHUNK_SIZE_APPEND_MB

        # Enforce limits
        target_mb = max(ChunkingManager.MIN_CHUNK_SIZE_MB, min(target_mb, ChunkingManager.MAX_CHUNK_SIZE_MB))

        # Calculate Memory Usage
        mem_usage_bytes = df.memory_usage(deep=True).sum()
        row_count = len(df)
        
        if mem_usage_bytes == 0 or row_count == 0:
             return 5000 # Fallback

        avg_row_size_bytes = mem_usage_bytes / row_count
        
        # Calculate rows based on target MB
        target_bytes = target_mb * 1024 * 1024
        rows_per_chunk = int(target_bytes / avg_row_size_bytes)
        
        # Ensure reasonable bounds
        rows_per_chunk = max(100, rows_per_chunk) # At least 100 rows

        logger.debug(f"Chunking Strategy -> size_based | limit={target_mb}MB | rows={rows_per_chunk}")
        
        return rows_per_chunk
