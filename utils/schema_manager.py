"""
Schema Manager Module

Provides functions for table creation, schema introspection, and schema evolution.
"""
import logging
import pandas as pd
from time import time
from sqlalchemy import create_engine, MetaData, Table, Column, String, Boolean, Date, Float, DateTime, Integer, inspect, text, BigInteger
from sqlalchemy.exc import NoSuchTableError

from utils.db_config_loader import get_db_connection_string, EngineManager
from utils.column_utils import clean_column_name, format_table_name, mask_connection_string, map_oracle_to_postgressql_type


def create_table_as_in_db(logger, table_name, as_table_name, engine, schema=None):
    """Deprecated: Use generate_select + session.execute(CREATE TABLE AS SELECT ... WITH NO DATA) instead."""
    target_metadata = MetaData(schema=schema)
    target_table = Table(as_table_name, target_metadata)
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name, schema=schema)
    ignore_columns = {'effective_date', 'end_date', 'is_current', 'etl_date_time', 'audit_id'}

    for column in columns:
        if column['name'] not in ignore_columns:
            c_type = column['type']
            col_copy = Column(column['name'], c_type)
            if hasattr(col_copy.type, 'collation'):
                col_copy.type.collation = None
            target_table.append_column(col_copy)

    target_metadata.create_all(engine)
    logger.info(f"Table created in db for the {as_table_name} in the schema {schema}")


def get_table_info_postgres(engine, schema, table_name):
    """Retrieves basic table metadata from a PostgreSQL-compatible database."""
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name, schema=schema)
    num_columns = len(columns)

    schema_table_name = format_table_name(schema, table_name)

    with engine.connect() as conn:
        row_count_query = f"SELECT COUNT(1) FROM {schema_table_name}"
        row_count = conn.execute(text(row_count_query)).scalar()

        size_query = f"SELECT pg_total_relation_size('{schema_table_name}')"
        table_size = conn.execute(text(size_query)).scalar()

    table_size_mb = table_size / (1024 * 1024) if table_size else 0

    return {
        "num_columns": num_columns,
        "row_count": row_count,
        "table_size_mb": table_size_mb
    }


def get_query_schema(connection_string, query, db_type, logger):
    """
    Executes the query with a LIMIT 0 wrapper to introspect the result schema.
    """
    engine = EngineManager.get_engine(connection_string)
    
    query = query.strip().rstrip(';')
    wrapped_query = f"SELECT * FROM ({query}) subq LIMIT 0"
    if db_type == 'oracle':
        wrapped_query = f"SELECT * FROM ({query}) subq WHERE 1=0"
    
    logger.info(f"Introspecting schema with: {wrapped_query}")

    temp_introspect_table = f"temp_introspect_{int(time())}"
    
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_introspect_table}"))
            if hasattr(conn, 'commit'):
                conn.commit()
            
            ctas_sql = f"CREATE TABLE {temp_introspect_table} AS {wrapped_query}"
            conn.execute(text(ctas_sql))
            if hasattr(conn, 'commit'):
                conn.commit()
            
        inspector = inspect(engine)
        columns_raw = inspector.get_columns(temp_introspect_table)
        
        mapped_columns = []
        for col in columns_raw:
            c_type = col['type']
            if db_type == 'oracle':
                c_type = map_oracle_to_postgressql_type(c_type)
            mapped_columns.append(Column(col['name'], c_type))
             
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {temp_introspect_table}"))
            if hasattr(conn, 'commit'):
                conn.commit()
             
        return mapped_columns

    except Exception as e:
        logger.warning(f"Error during temp-table introspection (CTAS): {e}. Falling back to Pandas type inference.")
        
        try:
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_introspect_table}"))
                if hasattr(conn, 'commit'):
                    conn.commit()
        except:
            pass

        try:
            df = pd.read_sql_query(wrapped_query, engine)
            
            mapped_columns = []
            for col_name, dtype in df.dtypes.items():
                c_type = String
                
                if pd.api.types.is_integer_dtype(dtype):
                    c_type = BigInteger
                elif pd.api.types.is_float_dtype(dtype):
                    c_type = Float
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    c_type = DateTime
                elif pd.api.types.is_bool_dtype(dtype):
                    c_type = Boolean
                
                mapped_columns.append(Column(col_name, c_type))
            
            return mapped_columns

        except Exception as fallback_error:
            logger.error(f"Fallback introspection also failed: {fallback_error}")
            raise e


def load_source_table_metadata(source_connection_string, source_schema, source_db_type, source_table_name, logger):
    """Loads metadata for a source table, handling Oracle type mapping."""
    source_engine = EngineManager.get_engine(source_connection_string)
    source_metadata = MetaData(schema=source_schema)

    if source_db_type == 'oracle':
        logger.debug(f"Loading Oracle metadata for {source_table_name}")
        source_table = Table(source_table_name, source_metadata)
        source_inspector = inspect(source_engine)
        source_columns = source_inspector.get_columns(source_table_name, schema=source_schema)

        for col in source_columns:
            col_name = col['name']
            col_type = col['type']
            col_type = map_oracle_to_postgressql_type(col_type)
            column = Column(col_name, col_type)
            source_table.append_column(column)
    else:
        source_table = Table(source_table_name, source_metadata, autoload_with=source_engine)

    logger.debug(f"Successfully loaded source table {source_table_name} metadata")
    return source_table


def ensure_schema_exists(engine, schema, logger):
    """Ensures the target schema exists in the database."""
    if not schema or schema == 'public':
        return

    try:
        with engine.connect() as connection:
            # Check if schema exists (PostgreSQL specific, but harmless if failed usually)
            # Simpler to just try creating it with IF NOT EXISTS which is supported by Postgres
            # For other DBs, this might fail, so we wrap in try-except
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            connection.commit()
            logger.info(f"Ensured schema '{schema}' exists.")
    except Exception as e:
        logger.warning(f"Could not create schema '{schema}': {e}. Proceeding, assuming it might exist or DB doesn't support this.")



def ensure_target_table_exists(entry, data, logger):
    """Ensures target table exists, creating it if necessary."""
    try:
        source_db_id = entry['source_db_id']
        target_db_id = entry['target_db_id']
        source_table_name = entry['source_table']
        target_table_name = entry['target_table']
        source_schema = entry['source_schema']
        target_schema = entry['target_schema']
        load_type = entry['load_type']
        file_flag = entry['file_flag']
        qualified_select = entry.get('qualified_select')
        auto_create_table = entry.get('auto_create_table', True)

        source_connection_string, source_db_type = get_db_connection_string(source_db_id, is_source=True)
        target_connection_string, target_db_type = get_db_connection_string(target_db_id, is_source=False)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Ensuring target table exists. Source: {mask_connection_string(source_connection_string)} ({source_schema}) -> Target: {mask_connection_string(target_connection_string)} ({target_schema})")
        
        if file_flag:
            source_table = None
            check_and_create_target_table(target_connection_string, target_table_name, target_schema, source_table, file_flag, load_type, data, auto_create_table=auto_create_table)
        elif qualified_select:
            logger.info("qualified_select present, using query introspection source schema.")
            source_columns = get_query_schema(source_connection_string, qualified_select, source_db_type, logger)
            check_and_create_target_table(target_connection_string, target_table_name, target_schema, None, file_flag, load_type, data, source_columns=source_columns, logger=logger, auto_create_table=auto_create_table)
        else:
            source_table = load_source_table_metadata(source_connection_string, source_schema, source_db_type, source_table_name, logger)
            check_and_create_target_table(target_connection_string, target_table_name, target_schema, source_table, file_flag, load_type, data, auto_create_table=auto_create_table)

    except Exception as e:
        logger.error(f"Error in ensuring target table exists details: {e}")
        raise e


def ensure_table_in_target_for_files(entry, data, log_id, logger):
    """Ensures target table exists for file-based loads."""
    target_table_name = entry['target_table']
    target_db_id = entry['target_db_id']
    target_schema = entry['target_schema']
    load_type = entry['load_type']

    target_connection_string, target_db_type = get_db_connection_string(target_db_id, is_source=False)
    logger.debug(f"Ensuring target table for files: {mask_connection_string(target_connection_string)} ({target_schema})")
    target_engine = EngineManager.get_engine(target_connection_string)
    target_metadata = MetaData(schema=target_schema)
    target_inspector = inspect(target_engine)

    if not target_inspector.has_table(target_table_name, schema=target_schema):
        columns = data.columns.tolist()
        if not columns:
            logger.error(f"DataFrame has no columns for log_id {log_id}")
            raise ValueError("DataFrame must have at least one column.")

        target_table = Table(target_table_name, target_metadata, *[Column(col, String) for col in columns])
        
        if load_type in ['scd2', 'scd1']:
            target_table.append_column(Column('row_id_hash', String(64)), replace_existing=True)
            target_table.append_column(Column('row_full_hash', String(64)), replace_existing=True)
            target_table.append_column(Column('is_current', Boolean, default=True), replace_existing=True)
            target_table.append_column(Column('effective_date', Date), replace_existing=True)
            target_table.append_column(Column('end_date', Date), replace_existing=True)
            target_table.append_column(Column('etl_date_time', DateTime, default=text('NOW()')), replace_existing=True)
            target_table.append_column(Column('audit_id', Integer), replace_existing=True)
        else:
            target_table.append_column(Column('row_full_hash', String(64)), replace_existing=True)
            target_table.append_column(Column('etl_date_time', DateTime, default=text('NOW()')), replace_existing=True)
            target_table.append_column(Column('audit_id', Integer), replace_existing=True)

        target_metadata.create_all(target_engine)
    else:
        _add_missing_etl_columns(target_engine, target_inspector, target_table_name, target_schema, load_type)


def _add_missing_etl_columns(target_engine, target_inspector, target_table_name, target_schema, load_type):
    """Helper to add missing ETL columns to existing table."""
    with target_engine.connect() as connection:
        schema_table_name = format_table_name(target_schema, target_table_name)
        existing_cols = {col['name'] for col in target_inspector.get_columns(target_table_name, schema=target_schema)}
        
        if load_type in ['scd2', 'scd1']:
            if 'is_current' not in existing_cols:
                connection.execute(text(f'ALTER TABLE {schema_table_name} ADD COLUMN "is_current" BOOLEAN DEFAULT TRUE'))
            if 'effective_date' not in existing_cols:
                connection.execute(text(f'ALTER TABLE {schema_table_name} ADD COLUMN "effective_date" DATE'))
            if 'end_date' not in existing_cols:
                connection.execute(text(f'ALTER TABLE {schema_table_name} ADD COLUMN "end_date" DATE'))
            if 'row_id_hash' not in existing_cols:
                connection.execute(text(f'ALTER TABLE {schema_table_name} ADD COLUMN "row_id_hash" VARCHAR(64)'))
        
        if 'etl_date_time' not in existing_cols:
            connection.execute(text(f'ALTER TABLE {schema_table_name} ADD COLUMN "etl_date_time" TIMESTAMP DEFAULT NOW()'))
        if 'audit_id' not in existing_cols:
            connection.execute(text(f'ALTER TABLE {schema_table_name} ADD COLUMN "audit_id" INTEGER'))
        if 'row_full_hash' not in existing_cols:
            connection.execute(text(f'ALTER TABLE {schema_table_name} ADD COLUMN "row_full_hash" VARCHAR(64)'))


def check_and_create_target_table(target_connection_string, target_table_name, target_schema, source_table, file_flag, load_type, data, source_columns=None, logger=None, auto_create_table=True):
    """Creates target table if it doesn't exist, with schema evolution support."""
    target_engine = EngineManager.get_engine(target_connection_string)
    target_inspector = inspect(target_engine)
    target_metadata = MetaData(schema=target_schema)

    if logger is None:
        logger = logging.getLogger(__name__)

    if not target_inspector.has_table(target_table_name, schema=target_schema):
        if not auto_create_table:
            logger.error(f"Table {target_schema}.{target_table_name} does not exist and auto_create_table is False.")
            raise NoSuchTableError(f"Table {target_schema}.{target_table_name} missing and auto-create disabled.")
        
        # Ensure schema exists before creating table
        ensure_schema_exists(target_engine, target_schema, logger)

        logger.warning(f"Table {target_schema}.{target_table_name} missing. Auto-creating...")
        target_table = Table(target_table_name, target_metadata)
        
        if file_flag:
            clean_columns = [clean_column_name(col) for col in data.columns]
            column_mapping = dict(zip(data.columns, clean_columns))
            data = data.rename(columns=column_mapping)

            target_table = Table(
                target_table_name,
                target_metadata,
                *[Column(col, String) for col in clean_columns],
                extend_existing=True
            )
            logger.info(f"Created table {target_schema}.{target_table_name} with columns: {clean_columns}")
        else:
            columns_src = source_columns if source_columns is not None else source_table.columns
            
            for column in columns_src:
                col_copy = Column(column.name, column.type)
                if hasattr(col_copy.type, 'collation'):
                    col_copy.type.collation = None
                target_table.append_column(col_copy)

        # Add ETL columns (use replace_existing=True in case source already has these columns)
        if load_type in ['scd2', 'scd1']:
            target_table.append_column(Column('row_id_hash', String(64)), replace_existing=True)
            target_table.append_column(Column('row_full_hash', String(64)), replace_existing=True)
            target_table.append_column(Column('is_current', Boolean, default=True), replace_existing=True)
            target_table.append_column(Column('effective_date', Date), replace_existing=True)
            target_table.append_column(Column('end_date', Date), replace_existing=True)
            target_table.append_column(Column('etl_date_time', DateTime, default=text('NOW()')), replace_existing=True)
            target_table.append_column(Column('audit_id', Integer), replace_existing=True)
        else:
            target_table.append_column(Column('row_full_hash', String(64)), replace_existing=True)
            target_table.append_column(Column('etl_date_time', DateTime, default=text('NOW()')), replace_existing=True)
            target_table.append_column(Column('audit_id', Integer), replace_existing=True)

        target_metadata.create_all(target_engine)
    else:
        # Schema evolution for existing tables
        _evolve_schema(target_engine, target_inspector, target_table_name, target_schema, 
                       source_table, source_columns, file_flag, load_type, data, logger)

    return data


def _evolve_schema(target_engine, target_inspector, target_table_name, target_schema, 
                   source_table, source_columns, file_flag, load_type, data, logger):
    """Adds missing columns to existing table."""
    with target_engine.connect() as connection:
        schema_table_name = format_table_name(target_schema, target_table_name)
        
        existing_cols_raw = target_inspector.get_columns(target_table_name, schema=target_schema)
        existing_cols_clean = {clean_column_name(c['name']) for c in existing_cols_raw}

        columns_to_sync = []
        if file_flag:
            columns_to_sync = [{'name': col, 'type': String()} for col in data.columns]
        elif source_table is not None:
            for col in source_table.columns:
                col_copy = Column(col.name, col.type)
                if hasattr(col_copy.type, 'collation'):
                    col_copy.type.collation = None
                columns_to_sync.append({'name': col.name, 'type': col.type})
        elif source_columns is not None:
            for col in source_columns:
                col_copy = Column(col.name, col.type)
                if hasattr(col_copy.type, 'collation'):
                    col_copy.type.collation = None
                columns_to_sync.append({'name': col.name, 'type': col.type})

        for col_info in columns_to_sync:
            col_name_clean = clean_column_name(col_info['name'])
            
            if col_name_clean not in existing_cols_clean:
                logger.info(f"Schema Evolution: Adding column '{col_name_clean}'")
                
                type_str = "VARCHAR"
                if not file_flag:
                    type_compiler = target_engine.dialect.type_compiler
                    type_str = type_compiler.process(col_info['type'])
                
                alter_stmt = text(f'ALTER TABLE {schema_table_name} ADD COLUMN "{col_name_clean}" {type_str}')
                try:
                    connection.execute(alter_stmt)
                    connection.commit()
                    existing_cols_clean.add(col_name_clean)
                except Exception as alt_err:
                    logger.error(f"Failed to add column {col_name_clean}: {alt_err}")
                    raise alt_err

        # Add missing ETL columns
        _add_etl_columns_if_missing(connection, schema_table_name, existing_cols_clean, load_type)
        connection.commit()


def _add_etl_columns_if_missing(connection, schema_table_name, existing_cols_clean, load_type):
    """Adds missing ETL system columns."""
    if load_type in ['scd2', 'scd1']:
        if 'is_current' not in existing_cols_clean:
            connection.execute(text(f'ALTER TABLE {schema_table_name} ADD COLUMN "is_current" BOOLEAN DEFAULT TRUE'))
        if 'effective_date' not in existing_cols_clean:
            connection.execute(text(f'ALTER TABLE {schema_table_name} ADD COLUMN "effective_date" DATE'))
        if 'end_date' not in existing_cols_clean:
            connection.execute(text(f'ALTER TABLE {schema_table_name} ADD COLUMN "end_date" DATE'))
        if 'row_id_hash' not in existing_cols_clean:
            connection.execute(text(f'ALTER TABLE {schema_table_name} ADD COLUMN "row_id_hash" VARCHAR(64)'))
    
    if 'etl_date_time' not in existing_cols_clean:
        connection.execute(text(f'ALTER TABLE {schema_table_name} ADD COLUMN "etl_date_time" TIMESTAMP DEFAULT NOW()'))
    if 'audit_id' not in existing_cols_clean:
        connection.execute(text(f'ALTER TABLE {schema_table_name} ADD COLUMN "audit_id" INTEGER'))
    if 'row_full_hash' not in existing_cols_clean:
        connection.execute(text(f'ALTER TABLE {schema_table_name} ADD COLUMN "row_full_hash" VARCHAR(64)'))
