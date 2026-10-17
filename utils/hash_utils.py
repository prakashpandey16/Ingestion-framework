"""
Hash Utilities Module

Provides row hashing functions for SCD2 and data integrity.
"""
import hashlib
import logging
import pandas as pd
from datetime import datetime

from decorators.decorators import log_execution_time, handle_errors
from utils.chunking_manager import ChunkingManager
from utils.column_utils import clean_column_name


def _compute_hashes(data, business_key, ignore_columns, chunk_size_mb, compute_row_id=True, logger=None):
    """
    Internal helper to compute row_id_hash and/or row_full_hash in chunks.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    rows_per_chunk = ChunkingManager.resolve_rows_per_chunk(data, 'hashing', explicit_chunk_mb=chunk_size_mb)
    num_chunks = max(1, len(data) // rows_per_chunk + (1 if len(data) % rows_per_chunk > 0 else 0))

    if compute_row_id:
        data['row_id_hash'] = None
    data['row_full_hash'] = None

    for i in range(num_chunks):
        start_row = i * rows_per_chunk
        end_row = min((i + 1) * rows_per_chunk, len(data))

        chunk = data.iloc[start_row:end_row].copy()
        chunk_hash = chunk.drop(columns=ignore_columns, errors='ignore').fillna('')

        try:
            if compute_row_id:
                bk_cols = [col for col in business_key if col in chunk.columns]
                # Deterministic Row ID Hash: concatenate business key values as strings,
                # then MD5. This is stable across sessions/runs unlike pd.util.hash_pandas_object
                # which uses a non-deterministic per-session seed.
                bk_concat = chunk[bk_cols].astype(str).apply(
                    lambda row: '|'.join(row.values), axis=1
                )
                data.loc[start_row:end_row-1, 'row_id_hash'] = [
                    hashlib.md5(v.encode()).hexdigest() for v in bk_concat
                ]

            # Deterministic Full Row Hash: same approach over all non-ignored columns
            full_concat = chunk_hash.astype(str).apply(
                lambda row: '|'.join(row.values), axis=1
            )
            data.loc[start_row:end_row-1, 'row_full_hash'] = [
                hashlib.md5(v.encode()).hexdigest() for v in full_concat
            ]

        except Exception as e:
            logger.error(f"Error processing hash chunk {i+1}: {str(e)}")
            raise
            
    return data


@handle_errors
@log_execution_time
def add_rowid_and_rowfull_hash(data, business_key, logger, chunk_size_mb):
    """
    Calculates row_id_hash and row_full_hash in a chunked, memory-safe manner.
    """
    logger.debug(f"*****Adding hash to inbound data based on {business_key}")
    ignore_columns = {'src_op_type', 'src_op_user', 'src_commit_ts', 'tgt_write_ts',
                      'bef_aft_intr', 'row_id_hash', 'row_full_hash'}

    if business_key is not None:
        business_key = [clean_column_name(col) for col in business_key if col and col.strip()]
    else:
        business_key = []

    if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)
    data.columns = [clean_column_name(col) for col in data.columns]

    if not business_key:
        logger.warning("No valid business keys provided. Using all columns for hashing.")
        business_key = [col for col in data.columns if col not in ignore_columns]

    logger.debug(f"Using normalized business keys for hashing: {business_key}")

    if 'row_id_hash' not in data.columns:
        missing_columns = [col for col in business_key if col not in data.columns]
        if missing_columns:
            error_msg = f"Business key columns not found in data: {missing_columns}. Available: {list(data.columns)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        data = _compute_hashes(data, business_key, ignore_columns, chunk_size_mb, compute_row_id=True, logger=logger)

    return data


def add_row_full_hash(data, logger, audit_id, chunk_size_mb=None):
    """Adds only the row_full_hash and ETL system columns (used for non-SCD loads)."""
    if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)

    ignore_columns = {'src_op_type', 'src_op_user', 'src_commit_ts', 'tgt_write_ts', 'bef_aft_intr'}

    if 'row_full_hash' not in data.columns:
        data = _compute_hashes(data, [], ignore_columns, chunk_size_mb, compute_row_id=False, logger=logger)

        data['etl_date_time'] = datetime.now()
        data['audit_id'] = audit_id

        logger.debug(f"After Adding row_full_hash columns {datetime.now()}")
    else:
        logger.info(f"Data passed already has hash id column. Skipping hash generation.")

    return data
