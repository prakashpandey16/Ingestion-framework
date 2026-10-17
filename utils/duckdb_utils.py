"""
DuckDB Utilities Module

Provides functions for DuckDB table operations.
"""
import gc
from sqlalchemy import inspect
from utils.column_utils import clean_column_name
from conf.config import config

# Read DuckDB settings from config
_chunk_config = config.get("chunking", {})
DUCKDB_CHUNK_SIZE_MB = _chunk_config.get("duckdb_chunk_size_mb", 600)


def create_table_in_duck_db(duckdb_conn, table_name, duck_db_table_name, engine, schema=None, dropETLcolumns=None):
    """Creates a table in DuckDB based on the schema of a remote table."""
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name, schema=schema)
    ignore_columns = {'effective_date', 'end_date', 'is_current', 'etl_date_time', 'audit_id'}

    column_definition = []
    for column in columns:
        col_name = column['name']
        col_type = column['type'].__class__.__name__.lower()
        duckdb_col_type = None

        if 'int' in col_type:
            duckdb_col_type = 'BIGINT' if 'big' in col_type else 'INTEGER'
        elif 'float' in col_type or 'double' in col_type:
            duckdb_col_type = 'DOUBLE'
        elif 'decimal' in col_type or 'numeric' in col_type:
            duckdb_col_type = 'DECIMAL'
        elif 'date' in col_type or 'time' in col_type:
            duckdb_col_type = 'TIMESTAMP'
        elif 'char' in col_type or 'text' in col_type or 'string' in col_type:
            duckdb_col_type = 'VARCHAR'
        else:
            duckdb_col_type = 'VARCHAR'

        if dropETLcolumns == 'Y' and col_name in ignore_columns:
            continue
        else:
            column_definition.append(f'"{col_name}" {duckdb_col_type}')

    create_table_sql = f"CREATE TABLE {duck_db_table_name} ( {', '.join(column_definition)})"
    duckdb_conn.execute(create_table_sql)


def insert_data_into_duckdb_table(duckdb_conn, data, table_name):
    """Loads a Pandas DataFrame into a DuckDB table."""
    data.columns = [clean_column_name(col) for col in data.columns]

    duckdb_conn.register('temp_df', data)
    duckdb_conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
    duckdb_conn.unregister('temp_df')
    gc.collect()


def unload_from_duckdb_intodb(duckdb_conn, duckdb_table_name, dbengine, db_table_name, logger, schema=None):
    """Unloads data from a DuckDB table back into the final target database."""
    load_chunk_size_mb = DUCKDB_CHUNK_SIZE_MB

    final_target_df = duckdb_conn.execute(f"SELECT * FROM {duckdb_table_name}").fetchdf()

    # Free up memory
    duckdb_conn.execute(f" DROP TABLE IF EXISTS {duckdb_table_name}")
    gc.collect()

    if final_target_df.empty:
        logger.info(f"DuckDB table {duckdb_table_name} was empty. No data to unload.")
        return

    mem_usage = final_target_df.memory_usage(deep=True).sum()
    loading_chunk_size = int(load_chunk_size_mb * 1024 * 1024 / mem_usage * len(final_target_df)) if mem_usage > 0 else 1000

    for i in range(0, len(final_target_df), loading_chunk_size):
        chunk = final_target_df.iloc[i:i+loading_chunk_size]
        chunk_num = i // loading_chunk_size + 1
        total_chunks = (len(final_target_df) + loading_chunk_size - 1) // loading_chunk_size

        chunk.to_sql(db_table_name, dbengine, schema=schema, if_exists='append', index=False)
        logger.info(f"Appended chunk {chunk_num}/{total_chunks} to the target db {db_table_name}")
