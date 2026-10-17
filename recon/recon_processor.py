import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, create_engine, text
from sqlalchemy.pool import NullPool
from datetime import datetime
from conf.config import engine, recon_config_table, recon_results_table, connection_string
from utils.column_utils import format_table_name
from utils.db_config_loader import get_db_connection_string
import hashlib

logger = logging.getLogger(__name__)

def get_recon_session():
    """
    Creates a new, non-pooled database session for recon operations.
    This is essential for fork safety when using multiprocessing.
    """
    recon_engine = create_engine(connection_string, poolclass=NullPool, connect_args={"sslmode": "require"})
    Session = sessionmaker(bind=recon_engine)
    return Session()

def fetch_recon_entries(metadata_id, time_to_run=None):
    session = get_recon_session()
    try:
        recon_query = select(recon_config_table).where(
            recon_config_table.c.metadata_id == metadata_id,
            recon_config_table.c.is_active == True
        )
        if time_to_run:
            recon_query = recon_query.where(recon_config_table.c.time_to_run.in_(['both', time_to_run]))

        recon_entries = session.execute(recon_query).mappings().all()
        return [dict(entry) for entry in recon_entries]
    except Exception as e:
        logger.error(f"Error fetching recon entries for metadata_id {metadata_id}: {e}", exc_info=True)
        session.rollback()
        return []
    finally:
        session.close()

def process_recon_entry(recon_entry, log_id):
    check_type = recon_entry['check_type']

    if check_type == 'row_count':
        return execute_row_count_check(recon_entry)
    elif check_type == 'aggregate_check':
        return execute_aggregate_check(recon_entry)
    elif check_type == 'distinct_count':
        return execute_distinct_count_check(recon_entry)
    elif check_type == 'sample_check':
        return execute_sample_check(recon_entry)
    else:
        logger.error(f"Unknown check type: {check_type}")
        return None

def execute_row_count_check(recon_entry):
    source_db_id = recon_entry['source_db_id']
    source_table_name = recon_entry['source_table_name']
    source_schema = recon_entry['source_schema']
    
    target_db_id = recon_entry['target_db_id']
    target_table_name = recon_entry['target_table_name']
    target_schema = recon_entry['target_schema']

    source_count = get_row_count(source_db_id, source_table_name, source_schema)
    target_count = get_row_count(target_db_id, target_table_name, target_schema)
    
    status = 'success' if source_count == target_count else 'failure'
    remarks = f"Source count: {source_count}, Target count: {target_count}"
    return create_recon_result(recon_entry, 'row_count', status, source_count, target_count, remarks)

def execute_aggregate_check(recon_entry):
    source_db_id = recon_entry['source_db_id']
    source_table_name = recon_entry['source_table_name']
    source_schema = recon_entry['source_schema']
    column_name = recon_entry['column_name']
    aggregation_function = recon_entry['aggregation_function']
    
    target_db_id = recon_entry['target_db_id']
    target_table_name = recon_entry['target_table_name']
    target_schema = recon_entry['target_schema']

    source_aggregate = get_aggregate_value(source_db_id, source_table_name, column_name, aggregation_function, source_schema)
    target_aggregate = get_aggregate_value(target_db_id, target_table_name, column_name, aggregation_function, target_schema)
    
    status = 'success' if source_aggregate == target_aggregate else 'failure'
    remarks = f"Source aggregate: {source_aggregate}, Target aggregate: {target_aggregate}"
    return create_recon_result(recon_entry, 'aggregate_check', status, source_aggregate, target_aggregate, remarks)

def execute_distinct_count_check(recon_entry):
    source_db_id = recon_entry['source_db_id']
    source_table_name = recon_entry['source_table_name']
    source_schema = recon_entry['source_schema']
    group_by_column = recon_entry['group_by_column']
    
    target_db_id = recon_entry['target_db_id']
    target_table_name = recon_entry['target_table_name']
    target_schema = recon_entry['target_schema']

    source_distinct_count = get_distinct_count(source_db_id, source_table_name, group_by_column, source_schema)
    target_distinct_count = get_distinct_count(target_db_id, target_table_name, group_by_column, target_schema)
    
    status = 'success' if source_distinct_count == target_distinct_count else 'failure'
    remarks = f"Source distinct count: {source_distinct_count}, Target distinct count: {target_distinct_count}"
    return create_recon_result(recon_entry, 'distinct_count', status, source_distinct_count, target_distinct_count, remarks)

def execute_sample_check(recon_entry):
    source_db_id = recon_entry['source_db_id']
    source_table_name = recon_entry['source_table_name']
    source_schema = recon_entry['source_schema']
    qualifying_column = recon_entry['qualifying_column']
    sample_size = recon_entry['sample_size']
    
    target_db_id = recon_entry['target_db_id']
    target_table_name = recon_entry['target_table_name']
    target_schema = recon_entry['target_schema']

    source_sample = get_sample_hash(source_db_id, source_table_name, qualifying_column, sample_size, source_schema)
    target_sample = get_sample_hash(target_db_id, target_table_name, qualifying_column, sample_size, target_schema)
    
    status = 'success' if source_sample == target_sample else 'failure'
    remarks = f"Source sample hash: {source_sample}, Target sample hash: {target_sample}"
    return create_recon_result(recon_entry, 'sample_check', status, source_sample, target_sample, remarks)

def get_row_count(db_id, table_name, schema):
    connection_string, db_type = get_db_connection_string(db_id, True)
    engine = create_engine(connection_string)
    connection = engine.connect()

    full_table_name = format_table_name(schema, table_name)
    query = f"SELECT COUNT(*) FROM {full_table_name}"
    result = connection.execute(text(query)).scalar()
    connection.close()
    return result

def get_aggregate_value(db_id, table_name, column_name, aggregation_function, schema):
    connection_string, db_type = get_db_connection_string(db_id, True)
    engine = create_engine(connection_string)
    connection = engine.connect()

    full_table_name = format_table_name(schema, table_name)
    query = f"SELECT {aggregation_function.upper()}({column_name}) FROM {full_table_name}"
    result = connection.execute(text(query)).scalar()
    connection.close()
    return result

def get_distinct_count(db_id, table_name, column_name, schema):
    connection_string, db_type = get_db_connection_string(db_id, True)
    engine = create_engine(connection_string)
    connection = engine.connect()

    full_table_name = format_table_name(schema, table_name)
    query = f"SELECT COUNT(DISTINCT {column_name}) FROM {full_table_name}"
    result = connection.execute(text(query)).scalar()
    connection.close()
    return result

def get_sample_hash(db_id, table_name, qualifying_column, sample_size, schema):
    connection_string, db_type = get_db_connection_string(db_id, True)
    engine = create_engine(connection_string)
    connection = engine.connect()

    full_table_name = format_table_name(schema, table_name)
    query = f"SELECT {qualifying_column} FROM {full_table_name} ORDER BY {qualifying_column} LIMIT {sample_size}"
    rows = connection.execute(text(query)).fetchall()
    connection.close()

    return hashlib.md5(str(rows).encode()).hexdigest()

def create_recon_result(recon_entry, check_type, status, source_value, target_value, remarks):
    session = get_recon_session()
    recon_result = {
        "recon_id": recon_entry['recon_id'],
        "run_timestamp": datetime.now(),
        "source_or_target": 'source',
        "check_type": check_type,
        "data_point_value": source_value if status == 'success' else None,
        "data_point_details": {"source_value": source_value, "target_value": target_value},
        "status": status,
        "remarks": remarks,
        "created_at": datetime.now()
    }
    try:
        session.execute(recon_results_table.insert().values(recon_result))
        session.commit()
    except Exception as e:
        logger.error(f"Error saving recon result: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()
    return recon_result
