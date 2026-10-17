"""
DataFrame Loader Module

Provides functions for loading DataFrames to database tables.
"""
import logging
import pandas as pd
from time import time
from sqlalchemy import inspect, text
from datetime import datetime, date

from utils.db_utils import retry_db_operation
from utils.chunking_manager import ChunkingManager
from utils.column_utils import clean_column_name, format_table_name

# Cache to store database schema definitions to avoid repeated inspector calls
_schema_cache = {}

def get_table_columns(engine, schema, table_name, logger):
    """Fetches table columns from the database, caching the result to avoid repeated inspector overhead."""
    cache_key = f"{schema}.{table_name}"
    if cache_key in _schema_cache:
        return _schema_cache[cache_key]

    try:
        inspector = inspect(engine)
        columns_info = inspector.get_columns(table_name, schema=schema)
        _schema_cache[cache_key] = columns_info
        return columns_info
    except Exception as e:
        logger.warning(f"Could not inspect table {schema}.{table_name}: {e}")
        return []

def drop_temp_table_if_exists(session, schema, table_name, logger):
    """Safely drops a temporary staging table if it exists and commits the change."""
    full_table_name = format_table_name(schema, table_name)
    logger.debug(f"Safely dropping staging table: {full_table_name}")
    try:
        session.execute(text(f"DROP TABLE IF EXISTS {full_table_name}"))
        session.commit()
        logger.debug(f"Dropped table {full_table_name} successfully (if it existed).")
    except Exception as e:
        logger.warning(f"Could not drop temp table {full_table_name}: {e}")


def generate_scd2_insert_sql_parts(session, target_schema, target_table, inbound_df_columns, audit_id, logger):
    """
    Generates the explicit column and value components for a robust SCD2 INSERT INTO... SELECT... statement.
    """
    # Get Target Table Column Names (Exact Case from DB)
    columns_info = get_table_columns(session.bind, target_schema, target_table, logger)
    target_cols_raw = [col['name'] for col in columns_info]

    # Map Target Columns to Inbound Data
    cleaned_inbound_cols = {clean_column_name(col) for col in inbound_df_columns}

    data_cols_target_exact = []
    data_select_cols_staging = []

    for raw_target_col in target_cols_raw:
        cleaned_target_col = clean_column_name(raw_target_col)
        if cleaned_target_col in cleaned_inbound_cols:
            data_cols_target_exact.append(raw_target_col)
            data_select_cols_staging.append(cleaned_target_col)

    # SCD2/System Columns and Static Values
    scd2_columns = [
        "is_current", "effective_date", "end_date", "etl_date_time", "audit_id"
    ]

    scd2_values_template = (
        f"True, CURRENT_DATE, DATE '9999-12-31', CURRENT_TIMESTAMP, {audit_id}"
    )

    # Build Final SQL Strings
    target_data_cols_quoted = [f'"{c}"' for c in data_cols_target_exact]
    target_scd2_cols_quoted = [f'"{c}"' for c in scd2_columns]
    full_target_cols_str = ", ".join(target_data_cols_quoted + target_scd2_cols_quoted)

    staging_select_cols_quoted = [f'tmp."{c}"' for c in data_select_cols_staging]
    full_select_vals_str = ", ".join(staging_select_cols_quoted) + f", {scd2_values_template}"

    return full_target_cols_str, full_select_vals_str


def generate_select(logger, table_name, engine, schema=None):
    """Generates a SELECT statement used to copy a table's schema (without ETL columns and data)."""
    ignore_columns = {'effective_date', 'end_date', 'is_current', 'etl_date_time', 'audit_id'}
    logger.info(f"inspecting table {table_name} from {schema}")
    
    columns = get_table_columns(engine, schema, table_name, logger)

    selected_columns = [f'"{col["name"]}"' for col in columns if col['name'] not in ignore_columns]

    if not selected_columns:
        logger.warning(f"No columns found for select statement for {table_name}.")
        return None

    schema_prefix = f"{schema}." if schema else ""
    select_statement = f"SELECT {','.join(selected_columns)} FROM {schema_prefix}{table_name} LIMIT 0"

    return select_statement


def unload_from_df_intodb(data: pd.DataFrame, dbengine, db_table_name: str, logger: logging.Logger, schema: str, chunksize_mb: int = 200):
    """
    Unload data from a pandas DataFrame to a database table in memory-efficient chunks.
    """
    if not isinstance(data, pd.DataFrame):
        if not data:
            logger.info(f"No data (empty list/None) to unload to {schema}.{db_table_name}")
            return
        data = pd.DataFrame(data)

    if data.empty:
        logger.info(f"No data (empty DataFrame) to unload to {schema}.{db_table_name}")
        return

    start_time = time()
    logger.info(f"Preparing to unload {len(data)} records to {schema}.{db_table_name}")

    try:
        columns_info = get_table_columns(dbengine, schema, db_table_name, logger)
        db_cols_exact = [col['name'] for col in columns_info]
        
        db_cols_cleaned = {clean_column_name(c): c for c in db_cols_exact}
        data.columns = [clean_column_name(col) for col in data.columns]
        
        valid_columns = [
            df_col for df_col in data.columns 
            if df_col in db_cols_cleaned
        ]

        if not valid_columns:
            raise ValueError("No valid columns to insert - column name mismatch with target table")
        
        data = data[valid_columns]
        rename_map = {clean: actual for clean, actual in db_cols_cleaned.items() if clean in data.columns}
        data = data.rename(columns=rename_map)
        
    except Exception as e:
        logger.error(f"Error aligning DataFrame to target table {schema}.{db_table_name}: {str(e)}")
        raise

    if len(data) > 0:
        try:
            rows_per_chunk = ChunkingManager.resolve_rows_per_chunk(data, 'unload', explicit_chunk_mb=chunksize_mb)
            loading_chunk_size = rows_per_chunk
            total_chunks = (len(data) + loading_chunk_size - 1) // loading_chunk_size
            
            for i in range(0, len(data), loading_chunk_size):
                chunk = data.iloc[i:i+loading_chunk_size]
                chunk_num = i // loading_chunk_size + 1
                
                logger.info(f"Appending chunk {chunk_num}/{total_chunks} to {schema}.{db_table_name} ({len(chunk)} rows)")
                
                # Use PostgreSQL COPY for faster bulk insert
                _copy_insert(chunk, dbengine, db_table_name, schema, logger)

        except Exception as e:
            logger.error(f"Error during data unload to {schema}.{db_table_name}: {str(e)}")
            raise
    
    end_time = time()
    if len(data) > 0:
        logger.info(f"Successfully loaded {len(data)} records in {end_time - start_time:.4f} seconds.")


def _cast_df_to_db_types(df: pd.DataFrame, engine, table_name: str, schema: str, logger) -> pd.DataFrame:
    """
    Cast DataFrame columns to match target database types.
    
    This prevents COPY failures when Pandas infers numeric columns as float64
    due to NULLs or mixed types (e.g., "1.0" instead of "1" for INTEGER columns).
    """
    try:
        columns_info = get_table_columns(engine, schema, table_name, logger)
        
        # Build a mapping of column name -> SQL type
        col_type_map = {col['name']: str(col['type']).upper() for col in columns_info}
        
        for col in df.columns:
            if col not in col_type_map:
                continue
            
            col_type = col_type_map[col]
            
            # Cast to nullable integer for INTEGER/BIGINT/SMALLINT columns
            if any(int_type in col_type for int_type in ['INTEGER', 'BIGINT', 'SMALLINT', 'INT']):
                if df[col].dtype in ['float64', 'float32']:
                    logger.debug(f"Casting column '{col}' from {df[col].dtype} to Int64 for COPY compatibility")
                    # Use pandas nullable Int64 to preserve NULLs
                    df[col] = df[col].astype('Int64')
        
        return df
        
    except Exception as e:
        logger.warning(f"Could not cast DataFrame types for COPY: {e}")
        return df


def _copy_insert(df: pd.DataFrame, engine, table_name: str, schema: str, logger):
    """Fast bulk insert using PostgreSQL COPY command."""
    from io import StringIO
    
    # Cast DataFrame columns to match database types (prevents float->int issues)
    df = _cast_df_to_db_types(df.copy(), engine, table_name, schema, logger)
    
    # Convert DataFrame to CSV in memory
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
    buffer.seek(0)
    
    # Get column list for COPY
    columns = ', '.join([f'"{col}"' for col in df.columns])
    copy_sql = f'COPY {schema}."{table_name}" ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', NULL \'\\N\')'
    
    try:
        raw_conn = engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                cur.copy_expert(copy_sql, buffer)
            raw_conn.commit()
        finally:
            raw_conn.close()
    except Exception as e:
        logger.warning(f"COPY failed, falling back to to_sql: {e}")
        # Fallback to original method
        df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)

