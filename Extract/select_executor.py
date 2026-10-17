import re
from sqlalchemy import create_engine, text, func, MetaData, Table, select
from sqlalchemy.orm import sessionmaker
from decorators.decorators import log_execution_time, handle_errors

@handle_errors
@log_execution_time
def add_watermark_condition(select_statement, watermark_column, watermark_value):
    watermark_condition = f"{watermark_column} > '{watermark_value}'"
    
    if re.search(r'\bWHERE\b', select_statement, re.IGNORECASE):
        modified_select_statement = re.sub(r'(?i)\bWHERE\b', f"WHERE {watermark_condition} AND", select_statement, 1)
    else:
        if re.search(r'\bORDER BY\b', select_statement, re.IGNORECASE):
            modified_select_statement = re.sub(r'(?i)\bORDER BY\b', f"WHERE {watermark_condition} ORDER BY", select_statement, 1)
        elif re.search(r'\bGROUP BY\b', select_statement, re.IGNORECASE):
            modified_select_statement = re.sub(r'(?i)\bGROUP BY\b', f"WHERE {watermark_condition} GROUP BY", select_statement, 1)
        else:
            modified_select_statement = f"{select_statement} WHERE {watermark_condition}"
    
    return modified_select_statement

@handle_errors
@log_execution_time
def get_max_watermark_value(target_table, watermark_column, connection_string, target_schema):
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    metadata = MetaData(schema=target_schema)
    session = Session()
    target = Table(target_table, metadata, autoload_with=engine)

    stmt = select(func.max(target.c[watermark_column]))
    result = session.execute(stmt).scalar()
    session.close()
    return result

@handle_errors
@log_execution_time
def execute_select(statement, connection_string):
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()

    results = session.execute(text(statement)).fetchall()
    session.close()
    return [dict(row._mapping) for row in results]

 

@handle_errors
@log_execution_time
def construct_select_statement(source_table, qualified_select, source_schema):
    if qualified_select:
        return qualified_select
    return f"SELECT * FROM {source_schema}.{source_table}" if source_schema else f"SELECT * FROM {source_table}"

@handle_errors
@log_execution_time
def construct_partitioned_select_statement(source_table, partition_column, partition_value, source_schema, qualified_select=None, chunk_size=None, source_db_type=None):
    partition_value_condition = None

    if chunk_size is not None and source_db_type is not None and chunk_size in [1, 2, 3]:
        if chunk_size == 1:  # Yearly
            if source_db_type == 'oracle':
                partition_value_condition = f"TO_CHAR({partition_column}, 'YYYY') = '{partition_value}'"
            elif source_db_type == 'postgresql':
                partition_value_condition = f"EXTRACT(YEAR FROM {partition_column}) = {partition_value}"
            elif source_db_type == 'mysql':
                partition_value_condition = f"YEAR({partition_column}) = {partition_value}"
        elif chunk_size == 2:  # Monthly
            year, month = partition_value.split('-')
            if source_db_type == 'oracle':
                partition_value_condition = f"TO_CHAR({partition_column}, 'YYYY-MM') = '{partition_value}'"
            elif source_db_type == 'postgresql':
                partition_value_condition = f"EXTRACT(YEAR FROM {partition_column}) = {year} AND EXTRACT(MONTH FROM {partition_column}) = {month}"
            elif source_db_type == 'mysql':
                partition_value_condition = f"YEAR({partition_column}) = {year} AND MONTH({partition_column}) = {month}"
        elif chunk_size == 3:  # Daily
            if source_db_type == 'oracle':
                partition_value_condition = f"TO_CHAR({partition_column}, 'YYYY-MM-DD') = '{partition_value}'"
            else:
                partition_value_condition = f"{partition_column} = '{partition_value}'"
    else:
        # Handle non-date partitioning or direct partition value
        if partition_value is None:
            partition_value_condition = f"{partition_column} IS NULL"
        else:
            partition_value_condition = f"{partition_column} = '{partition_value}'"

    # Build the final query
    if qualified_select:
        if 'WHERE' in qualified_select.upper():
            return f"{qualified_select} AND {partition_value_condition}"
        else:
            return f"{qualified_select} WHERE {partition_value_condition}"
    else:
        return f"SELECT * FROM {source_schema}.{source_table} WHERE {partition_value_condition}" if source_schema else f"SELECT * FROM {source_table} WHERE {partition_value_condition}"
