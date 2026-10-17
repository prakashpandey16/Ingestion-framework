"""
SCD2 Loader Module

This module provides the SCD2Loader class and associated utilities for handling
Slowly Changing Dimensions Type 2 (SCD2) loading processes. It supports multiple
execution engines including Target DB (SQL), DuckDB, and Pandas.
"""
import hashlib
from datetime import date, datetime
from sqlalchemy import text
from load_handlers.base_loader import BaseLoader
from load_handlers.dlt_loader import DLTSCD2Loader
from load_handlers.scd2_load_helper import load_data_with_pandas
from decorators.decorators import log_execution_time, handle_errors
from utils.logging_utils import log_error_details, send_failure_notification
from conf.config import engine
from sqlalchemy.orm import sessionmaker
import connectorx as cx
import numpy as np
import logging
import duckdb
import pandas as pd
import gc
from pathlib import Path


# Import from split utility modules
from utils.column_utils import clean_column_name
from utils.hash_utils import add_rowid_and_rowfull_hash
from utils.dataframe_loader import (
    generate_select,
    unload_from_df_intodb,
    drop_temp_table_if_exists,
    generate_scd2_insert_sql_parts
)
from utils.duckdb_utils import (
    create_table_in_duck_db,
    insert_data_into_duckdb_table,
    unload_from_duckdb_intodb
)
from utils.chunking_manager import ChunkingManager
from utils.db_utils import retry_db_operation
from utils.db_config_loader import EngineManager

from conf.config import config

# Configuration Setup
try:
    _config = config
    use_duckdb = _config["scd2engine"].get("use_duckdb", False)
    use_pandas = _config["scd2engine"].get("use_pandas", False)
    use_target_db = _config["scd2engine"].get("use_target_db", False)
except Exception as e:
    # Fallback default (though this should ideally not be reached if config is correct)
    logger.warning(f"Config issue in SCD2Loader: {e}. Defaulting to target_db engine.")
    use_duckdb = False
    use_pandas = False
    use_target_db = True

logger = logging.getLogger(__name__)



def mangle_duplicate_columns(columns):
    """
    Appends .1, .2, etc. to duplicate column names to ensure uniqueness.
    
    Robust implementation that handles cases where potential suffixed names 
    (e.g., 'col.1') already exist in the input list.
    """
    counts = {}
    used_names = set()
    new_cols = []
    
    for col in columns:
        if col not in used_names:
            # First occurrence, and no collision
            new_cols.append(col)
            used_names.add(col)
            counts[col] = 0
        else:
            # Name collision (either it's a true duplicate or a collision with a previous name)
            original_base = col
            counts[original_base] = counts.get(original_base, 0) + 1
            
            # Start attempting suffixes
            while True:
                candidate = f"{original_base}.{counts[original_base]}"
                if candidate not in used_names:
                    new_cols.append(candidate)
                    used_names.add(candidate)
                    break
                else:
                    # If the candidate suffix also exists (e.g. input had both 'A' and 'A.1')
                    # Increment and try again until we find a free slot
                    counts[original_base] += 1
                    
    return new_cols


class SCD2Loader(BaseLoader):
    """
    Loader class for handling Slowly Changing Dimensions Type 2 (SCD2).
    """
    def __init__(self, target_table, use_connectorx, connection_string, target_schema, business_key_columns, use_hash, delete_handling=None):
        logger.info(f"SCD2Loader initializing for table: {target_table} in schema: {target_schema}")
        super().__init__(target_table, use_connectorx, connection_string, target_schema)
        self.business_key_columns = business_key_columns
        self.use_hash = use_hash
        self.delete_handling = delete_handling
        logger.info(f"SCD2Loader initialized with BKeys: {business_key_columns}")

    def load_data_default(self, entry, use_connectorx, data, audit_id, source_keys=None, delete_table_name=None):
        """
        Orchestrates the load process based on the configured engine (DuckDB, Pandas, or Target DB).

        Args:
            entry (dict): Configuration entry.
            use_connectorx (bool): Whether to use ConnectorX.
            data: The data to load (DataFrame or other format).
            audit_id (str): Audit identifier.
            source_keys (list, optional): Keys from source.
            delete_table_name (str, optional): Name of the table handling deletes.

        Returns:
            tuple: (insert_count, update_count, delete_count)
        """
        if use_duckdb:
            insert_count, update_count, delete_count = load_data_with_duckdb_chunked(self, entry, data, audit_id, source_keys, delete_table_name)
        elif use_pandas:
            insert_count, update_count = load_data_with_pandas(self, data, audit_id, source_keys, delete_table_name)
            delete_count = 0
        elif use_target_db:
            insert_count, update_count, delete_count = load_data_with_db(self, entry, data, audit_id, 'N', source_keys, delete_table_name)

        return insert_count, update_count, delete_count

    def load_data_with_dlt(self, data, audit_id):
        dlt_loader = DLTSCD2Loader(self.target.name, self.engine.url, self.metadata.schema)
        dlt_loader.load_data(data, audit_id)


@handle_errors
def load_data_with_db(self, entry, data, audit_id, use_temp=None, source_keys=None, delete_table_name=None):
    """
    Handles SCD Type 2 logic entirely within the target database using SQL staging tables.

    Args:
        self (SCD2Loader): The loader instance.
        entry (dict): Configuration entry.
        data (pd.DataFrame): Data to load.
        audit_id (str): Audit identifier.
        use_temp (str, optional): 'Y' to use temporary tables.
        source_keys (list, optional): Keys from source.
        delete_table_name (str, optional): Name of table handling deletes.

    Returns:
        tuple: (insert_count, update_count, delete_count)
    """
    logger.debug(f"Starting load_data_with_db for table: {self.target_table}")
    
    # Initialize separate counters
    total_inserts = 0
    total_updates = 0
    total_deletes = 0

    target_table_name = self.target_table
    # Unique staging table names to prevent lock contention
    temp_table_name = f"temp_table_{self.target_table}_{audit_id}"
    temp_delete_table = f"temp_delete_table_{self.target_table}_{audit_id}"

    # Temp table management - drop ensures we start clean even if a previous run crashed
    drop_temp_table_if_exists(self.session, self.metadata.schema, temp_table_name, logger)
    drop_temp_table_if_exists(self.session, self.metadata.schema, temp_delete_table, logger)

    try:
        if use_temp == 'Y':
            target_table_name = f"{self.target_table}_temp"
            self.session.execute(
                text(f"CREATE UNLOGGED TEMPORARY TABLE {self.metadata.schema}.{target_table_name} "
                     f"AS SELECT * FROM {self.metadata.schema}.{self.target_table} LIMIT 0")
            )

        select_statement = generate_select(logger, self.target_table, self.engine, self.metadata.schema)
        # Use UNLOGGED tables for performance
        self.session.execute(text(f"CREATE UNLOGGED TABLE {self.metadata.schema}.{temp_table_name} AS {select_statement} WITH NO DATA"))
        self.session.execute(text(f"CREATE UNLOGGED TABLE {self.metadata.schema}.{temp_delete_table} AS {select_statement} WITH NO DATA"))
        self.session.commit()

        # Target Table Indexes: Composite and Partial for fast change detection
        # Creating these if they don't exist ensures high-performance SCD2 queries
        idx_current_name = f"idx_scd2_{self.target_table}_current"
        idx_current_hash_name = f"idx_scd2_{self.target_table}_hash"
        # Business key index for Expire UPDATE and Insert NOT EXISTS JOINs
        bk_cols_quoted = ', '.join([f'"{col}"' for col in self.business_key_columns])
        idx_bk_current_name = f"idx_scd2_{self.target_table}_bk_current"
        try:
            self.session.execute(text(f'CREATE INDEX IF NOT EXISTS "{idx_current_name}" ON {self.metadata.schema}.{target_table_name} ("row_id_hash") WHERE is_current = TRUE'))
            self.session.execute(text(f'CREATE INDEX IF NOT EXISTS "{idx_current_hash_name}" ON {self.metadata.schema}.{target_table_name} ("row_id_hash", "row_full_hash") WHERE is_current = TRUE'))
            # Partial index on business key columns — directly supports the JOIN conditions
            # used in Expire (UPDATE ... WHERE bk_join) and Insert (NOT EXISTS ... WHERE bk_join)
            self.session.execute(text(f'CREATE INDEX IF NOT EXISTS "{idx_bk_current_name}" ON {self.metadata.schema}.{target_table_name} ({bk_cols_quoted}) WHERE is_current = TRUE'))
            self.session.commit()
            logger.info(f"Ensured target table indexes exist: {idx_current_name}, {idx_current_hash_name}, {idx_bk_current_name}")
        except Exception as e:
            logger.warning(f"Could not create target table partial indexes: {e}")
            self.session.rollback()

        # Data processing
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)

        if data.empty:
            logger.info(f"Dataset for {self.target_table} is empty. Skipping SCD2 load operations.")
            return total_inserts, total_updates, total_deletes

        # Drop truly identical rows incoming from Source CSV
        # We keep 'last' to have the freshest exact row in case of minor timestamp variations
        data = data.drop_duplicates(keep='last')
        
        order_col_name = None
        # Process sequential updates without deduplicating business keys
        if hasattr(self, 'business_key_columns') and self.business_key_columns:
            order_col_raw = entry.get('order_by_time')
            if order_col_raw and order_col_raw in data.columns:
                order_col_name = clean_column_name(order_col_raw)
                data = data.sort_values(by=[*self.business_key_columns, order_col_raw])
        
        # Clean & normalize column names (handles D+1 → d_1)
        data.columns = [clean_column_name(c) for c in data.columns]

        # Ensure unique columns (handles duplicates like duplicate 'sla')
        if pd.Series(data.columns).duplicated().any():
            logger.warning("Duplicate column names detected after cleaning — mangling duplicates to make unique.")
            data.columns = mangle_duplicate_columns(list(data.columns))
            logger.info(f"Columns after mangling: {list(data.columns)}")

        # Hashing logic
        # Passing chunk_size_mb=None triggers ChunkingManager default (75MB for SCD2)
        chunk_strategy = f"size_based | scd2_limit={ChunkingManager.DEFAULT_CHUNK_SIZE_SCD2_MB}MB | resolved_rows={len(data)}"
        logger.info(f"Chunking strategy: {chunk_strategy}")
        
        data = add_rowid_and_rowfull_hash(data, self.business_key_columns, logger, chunk_size_mb=None)

        # Safety Check: Detect multiple intraday updates
        if 'row_id_hash' in data.columns:
            duplicate_counts = data.groupby('row_id_hash').size()
            duplicates = duplicate_counts[duplicate_counts > 1]
            if not duplicates.empty:
                logger.info(f"Batch safety check: Found {len(duplicates)} business keys with multiple intraday updates.")

        # Local parquet backup
        self.save_backup(data, entry, self.target_table)

        # Segregate updates & deletes
        data_delete = pd.DataFrame()
        if 'src_op_type' in list(data.columns):
            data_delete = data[data['src_op_type'] == 'D'].copy()
            data = data[data['src_op_type'] != 'D'].copy()
            delete_count = len(data_delete)
        else:
            delete_count = 0

        # Load into staging tables
        unload_from_df_intodb(data, self.engine, temp_table_name, logger, self.metadata.schema, chunksize_mb=ChunkingManager.DEFAULT_CHUNK_SIZE_SCD2_MB)
        if not data_delete.empty:
            unload_from_df_intodb(data_delete, self.engine, temp_delete_table, logger, self.metadata.schema)

        # Performance: Add indexes on temp table for faster JOINs
        # Use unique index name per table/audit_id to prevent conflicts in parallel execution
        index_name = f"idx_temp_{self.target_table}_{audit_id}_row_id"
        bk_index_name = f"idx_temp_{self.target_table}_{audit_id}_bk"
        bk_cols_quoted = ', '.join([f'"{col}"' for col in self.business_key_columns])
        logger.info(f"Creating indexes on {temp_table_name} for faster SCD2 operations...")
        self.session.execute(text(f'CREATE INDEX {index_name} ON {self.metadata.schema}.{temp_table_name} ("row_id_hash")'))
        # Business key index on temp table — speeds up CTE PARTITION BY and JOIN from target side
        self.session.execute(text(f'CREATE INDEX {bk_index_name} ON {self.metadata.schema}.{temp_table_name} ({bk_cols_quoted})'))
        
        logger.info(f"Analyzing {temp_table_name} and target table to update Postgres planner statistics...")
        self.session.execute(text(f'ANALYZE {self.metadata.schema}.{temp_table_name}'))
        self.session.execute(text(f'ANALYZE {self.metadata.schema}.{target_table_name}'))
        


        # SCD2 SQL Logic
        # Build business key JOIN conditions using actual columns (not row_id_hash)
        # This is robust against any historical hash drift between runs
        bk_join_condition = ' AND '.join(
            [f't."{col}" = tmp."{col}"' for col in self.business_key_columns]
        )
        row_id_hash_condition = f't."row_id_hash" = tmp."row_id_hash"'  # kept for delete handling

        inbound_cols = list(data.columns)
        full_target_cols_str, full_select_vals_str = generate_scd2_insert_sql_parts(
            self.session, self.metadata.schema, self.target_table, inbound_cols, audit_id, logger
        )

        # Extract the data columns only (strip trailing SCD2 static values)
        staging_cols_only = full_select_vals_str.split(', True')[0]

        # Partition CTE by actual business key columns for reliable ordering
        bk_partition_cols = ', '.join([f'"{col}"' for col in self.business_key_columns])
        order_by_sql = f'ORDER BY "{order_col_name}"' if order_col_name else 'ORDER BY "row_full_hash"'
        next_time_col_sql = f', LEAD("{order_col_name}") OVER (PARTITION BY {bk_partition_cols} {order_by_sql}) AS next_time' if order_col_name else ''

        # Reusable CTE definition inlined into each query
        ordered_cte = f"""
            WITH ordered_batch AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY {bk_partition_cols} {order_by_sql}) AS rn,
                       ROW_NUMBER() OVER (PARTITION BY {bk_partition_cols} {order_by_sql} DESC) AS rn_desc
                       {next_time_col_sql}
                FROM {self.metadata.schema}.{temp_table_name}
            )
        """

        end_date_expr = f'CAST(tmp."{order_col_name}" AS DATE)' if order_col_name else "CURRENT_DATE"
        next_time_expr = 'CAST(tmp.next_time AS DATE)' if order_col_name else "DATE '9999-12-31'"

        # Step 1: Expire old versions — join on business keys directly (not row_id_hash)
        # Using business keys makes this robust against any hash drift between runs
        logger.info(f"Expiring old versions in {target_table_name} using ordered window functions...")
        res_expire = self.session.execute(text(f"""
            {ordered_cte}
            UPDATE {self.metadata.schema}.{target_table_name} t
            SET is_current = False,
                end_date = COALESCE({end_date_expr}, CURRENT_DATE)
            FROM ordered_batch tmp
            WHERE {bk_join_condition}
              AND t.is_current = True
              AND tmp.rn = 1
              AND (t."row_full_hash" IS DISTINCT FROM tmp."row_full_hash" OR tmp.rn_desc > 1)
        """))
        expired_count = res_expire.rowcount
        total_updates += expired_count
        logger.info(f"Expired {expired_count} old versions.")

        # Handle deletes (UPDATE)
        if not data_delete.empty:
            res_delete_update = self.session.execute(text(f"""
                UPDATE {self.metadata.schema}.{target_table_name} t
                SET is_current = False, end_date = CURRENT_DATE
                FROM {self.metadata.schema}.{temp_delete_table} tmp
                WHERE {row_id_hash_condition}
                  AND t.is_current = True
            """))
            total_updates += res_delete_update.rowcount

        # Step 2 & 3: Insert sequenced new versions + truly new keys
        # NOT EXISTS uses business key join to skip insertion of records
        # where an identical current version (same full hash) already exists
        logger.info(f"Inserting sequenced records into {target_table_name}...")
        res_insert_all = self.session.execute(text(f"""
            {ordered_cte}
            INSERT INTO {self.metadata.schema}.{target_table_name} ({full_target_cols_str})
            SELECT {staging_cols_only},
                   CASE WHEN tmp.rn_desc = 1 THEN True ELSE False END AS is_current,
                   COALESCE({end_date_expr}, CURRENT_DATE) AS effective_date,
                   COALESCE({next_time_expr}, DATE '9999-12-31') AS end_date,
                   CURRENT_TIMESTAMP AS etl_date_time,
                   {audit_id} AS audit_id
            FROM ordered_batch tmp
            WHERE NOT EXISTS (
                SELECT 1 FROM {self.metadata.schema}.{target_table_name} t
                WHERE {bk_join_condition}
                  AND t."row_full_hash" IS NOT DISTINCT FROM tmp."row_full_hash"
                  AND t.is_current = True
            )
        """))
        truly_new_inserts = res_insert_all.rowcount
        new_version_inserts = 0  # Merged into single INSERT
        total_inserts += truly_new_inserts
        logger.info(f"Inserted {truly_new_inserts} sequenced records.")
        
        if use_temp == 'Y':
            backupandreplace(self)

        self.session.commit()

        # Final Summary Log - Corrected calculation
        # unchanged = total input records - truly new records - records that were updated (expired)
        input_record_count = len(data)
        unchanged_count = input_record_count - truly_new_inserts - expired_count
        logger.info(f"\n"
                    f"Table: {self.target_table}\n"
                    f"Rows processed: {input_record_count}\n"
                    f"Insert: {truly_new_inserts} | Updated: {new_version_inserts} | "
                    f"Expired: {expired_count} | Deleted: {total_deletes}\n")
                    
        return total_inserts, total_updates, total_deletes

    finally:
        try:
           self.session.close()
        except:
           pass
        # Always clean up temp tables, even if errors occur
        # Use a fresh connection from EngineManager (using self.engine as it's the same pool)
        # Use getattr to prevent AttributeError if self.engine wasn't initialized
        engine = getattr(self, 'engine', None)
        if engine:
            try:
                with engine.connect() as conn:
                    drop_temp_table_if_exists(conn, self.metadata.schema, temp_table_name, logger)
                    drop_temp_table_if_exists(conn, self.metadata.schema, temp_delete_table, logger)
                logger.info(f"Dropped temp tables: {temp_table_name}, {temp_delete_table}")
            except Exception as e:
                logger.warning(f"Failed to drop temp tables in finally block: {e}")


def backupandreplace(self):
    """
    Handles replacing the main table with the temp table during full loads (if use_temp='Y').
    
    This function:
    1. Drops any existing backup table.
    2. Renames the current table to the backup table.
    3. Renames the temp table to the main table.
    """

    schema_table_name, schema_table_name_backup, schema_table_name_temp = get_scd2_table_names(self.metadata.schema, self.target_table)
    with self.engine.connect() as conn:
        logger.info(f"Swapping tables: backup {schema_table_name} -> {schema_table_name_backup}, promote {schema_table_name_temp} -> {schema_table_name}")
        conn.execute(text(f"DROP TABLE IF EXISTS {schema_table_name_backup}"))
        conn.execute(text(f"ALTER TABLE {schema_table_name} RENAME TO {self.target_table}_backup"))
        conn.execute(text(f"ALTER TABLE {schema_table_name_temp} RENAME TO {self.target_table}"))

def get_scd2_table_names(schema, table):
    """
    Helper to return quoted, schematized table names for backup/temp.
    
    Args:
        schema (str): The schema name.
        table (str): The table name.

    Returns:
        tuple: (schema_table_name, schema_table_name_backup, schema_table_name_temp)
    """
    if schema:
        return f'"{schema}"."{table}"', f'"{schema}"."{table}_backup"', f'"{schema}"."{table}_temp"'
    else:
        return f'{table}', f'{table}_backup', f'{table}_temp'

def sanitize_datetime(df):
    """
    Sanitizes datetime columns in a DataFrame (placeholder).

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Sanitized DataFrame.
    """
    min_datetime = datetime(1678, 9, 21)
    max_datetime = datetime(2262, 4, 11)
    return df

@handle_errors
def load_data_with_duckdb_chunked(self, entry, data, audit_id, source_keys=None, delete_table_name=None):
    """
    Handles load using DuckDB for memory-efficient processing (placeholder).

    Args:
        self (SCD2Loader): Loader instance.
        entry (dict): Configuration entry.
        data: Input data.
        audit_id (str): Audit identifier.
        source_keys (list, optional): Keys from source.
        delete_table_name (str, optional): Name of table handling deletes.

    Returns:
        tuple: (insert_count, update_count, delete_count)
    """
    return 0, 0, 0

def checkcount(duckdb_conn, table_name):
    """
    Helper function to get row count from a DuckDB table.

    Args:
        duckdb_conn: DuckDB connection object.
        table_name (str): Name of the table.

    Returns:
        int: Count of rows.
    """
    return duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
