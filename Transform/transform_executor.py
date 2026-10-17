import logging
from sqlalchemy import create_engine, text, func, MetaData, Table, select
from sqlalchemy.orm import sessionmaker
from decorators.decorators import log_execution_time, handle_errors
from conf.config import engine, column_operations_table, enumerated_types_table
import pandas as pd

def apply_enum_conversion(session, data, source_column, target_column, enum_id):
    enum_values = session.execute(
        enumerated_types_table.select().where(enumerated_types_table.c.enum_id == enum_id)
    ).fetchall()
    
    enum_dict = {row['enum_code']: row['enum_value'] for row in enum_values}

    if isinstance(data, pd.DataFrame):
        data[target_column] = data[source_column].map(enum_dict).fillna(data[source_column])
    else:
        for record in data:
            if source_column in record:
                record[target_column] = enum_dict.get(record[source_column], record[source_column])

def transform_data(data, column_operations_key, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)
    
    is_dataframe = isinstance(data, pd.DataFrame)
    if not is_dataframe:
        data = pd.DataFrame(data)
    
    Session = sessionmaker(bind=engine)
    session = Session()
    operations = session.execute(
        column_operations_table.select().where(column_operations_table.c.id == column_operations_key)
    ).fetchall()

    for operation in operations:
        if operation.operation_type == 'add':
            if operation.enum_id and operation.source_column:
                logger.info(f"Converting column {operation.source_column} to {operation.column_name} using enum {operation.enum_id}")
                apply_enum_conversion(session, data, operation.source_column, operation.column_name, operation.enum_id)
            else:
                logger.info(f"Adding column {operation.column_name} with SQL syntax {operation.sql_syntax}")
                data[operation.column_name] = data.apply(lambda row: eval(operation.sql_syntax.format(**row)), axis=1)
        elif operation.operation_type == 'drop':
            logger.info(f"Dropping column {operation.column_name}")
            data = data.drop(columns=[operation.column_name])

    session.close()
    return data if is_dataframe else data.to_dict(orient='records')
