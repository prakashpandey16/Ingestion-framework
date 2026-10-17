from deltalake import write_deltalake
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

log = logging.getLogger(__name__)

def save_delta_copy(df: pd.DataFrame, 
                   destination_path: str, 
                   storage_options: dict = None, 
                   mode: str = 'append',
                   partition_by: list = None) -> str | int:
    """
    Saves a DataFrame as a Delta Table to the specified path (Local or S3).
    
    Args:
        df: DataFrame to save.
        destination_path: Full path to the Delta table (e.g., "s3://bucket/table" or "./data/table").
        storage_options: Dictionary containing AWS credentials and region (required for S3).
        mode: Write mode ('append' or 'overwrite'). Default is 'append'.
        partition_by: List of columns to partition by.
        
    Returns:
        The destination path on success, or 0 on failure.
    """
    try:
        log.info(f"Saving Delta table to: {destination_path} (Mode: {mode}, Partitioned: {bool(partition_by)})")
        
        # 1. Handle All-Null Columns (Delta Lake requires concrete types)
        if not df.empty:
            for col in df.columns:
                # Check if entire column is null
                if df[col].isnull().all():
                    # Option B: Cast to String and fill with "N/A" to ensure valid Delta schema
                    df[col] = df[col].astype(str).replace('nan', 'N/A').replace('None', 'N/A')
                    continue

                # Check for object or float type columns (to avoid decimal inference issues)
                if df[col].dtype == 'object' or 'float' in str(df[col].dtype):
                     # Cast to string to avoid ambiguous inference (e.g. Decimal overflow for numbers)
                     df[col] = df[col].astype(str)
                     # Clean up 'nan' and 'None' strings back to actual None
                     df[col] = df[col].replace(['nan', 'None'], None)
        
        write_deltalake(
            destination_path,
            df,
            mode=mode,
            partition_by=partition_by,
            storage_options=storage_options,
            schema_mode='merge'
        )
        
        log.info(f"Successfully saved Delta table to: {destination_path}")
        return destination_path

    except Exception as e:
        log.error(f"Failed to save Delta table to {destination_path}: {e}", exc_info=True)
        return 0
    
