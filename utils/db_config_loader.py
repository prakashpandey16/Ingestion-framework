from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import sessionmaker
from conf.config import engine, source_db_config_table, target_db_config_table, EngineManager
from utils.crypto_utils import decrypt_value
import logging

logger = logging.getLogger(__name__)



def get_db_engine(db_id, is_source=True):
    """
    Retrieves or creates a cached SQLAlchemy engine for the given db_id.
    Recommended over get_db_connection_string for all execution logic.
    """
    connection_string, db_type = get_db_connection_string(db_id, is_source)
    return EngineManager.get_engine(connection_string, db_type)


Session = sessionmaker(bind=engine)
session = Session()

def get_db_connection_string(db_id, is_source=True):
    # Determine the appropriate table
    table = source_db_config_table if is_source else target_db_config_table

    # Fetch the database configuration
    stmt = table.select().where(table.c.id == db_id)
    result = session.execute(stmt).fetchone()

    if not result:
        raise ValueError(f"No database configuration found for id: {db_id}")

    # Convert the result tuple to a dictionary
    db_config = {column.name: value for column, value in zip(table.c, result)}

    # Build the connection string
    connection_string = decrypt_value(db_config['connection_string'])
    db_type=db_config['db_type']
    return connection_string,db_type
def get_aws_access_secret_key(db_id,is_source=True):
    table = source_db_config_table if is_source else target_db_config_table
   # Fetch the database configuration
    stmt = table.select().where(table.c.id == db_id)
    result = session.execute(stmt).fetchone()

    if not result:
        raise ValueError(f"No database configuration found for id: {db_id}")

    # Convert the result tuple to a dictionary
    db_config = {column.name: value for column, value in zip(table.c, result)}

    # Build the connection string
    s3_access_key = decrypt_value(db_config['s3_access_key'])
    s3_secret_key =decrypt_value(db_config['s3_secret_key'])
    return s3_access_key,s3_secret_key

