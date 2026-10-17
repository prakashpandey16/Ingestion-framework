"""
Column Utilities Module

Provides column name cleaning, type mapping, and formatting utilities.
"""
import re
from sqlalchemy import String, Boolean, Date, Float, DateTime, Integer, BigInteger


def mask_connection_string(conn_str: str) -> str:
    """Masks credentials in connection strings for safe logging."""
    if not conn_str:
        return conn_str
    return re.sub(r'://[^:]+:[^@]+@', '://***:***@', conn_str)


def map_oracle_to_postgressql_type(oracle_type):
    """Maps Oracle/generic column types to appropriate PostgreSQL/SQLAlchemy types."""
    if isinstance(oracle_type, Integer):
        return BigInteger
    elif isinstance(oracle_type, String):
        return String
    elif isinstance(oracle_type, Float):
        return Float
    elif isinstance(oracle_type, Date):
        return Date
    elif isinstance(oracle_type, DateTime):
        return DateTime
    else:
        type_name = getattr(oracle_type, '__class__', None).__name__.lower()
        if 'int' in type_name:
            return BigInteger
        elif 'float' in type_name or 'double' in type_name or 'number' in type_name:
            return Float
        elif 'date' in type_name or 'timestamp' in type_name:
            return DateTime
        return String


def format_table_name(schema, table):
    """Formats schema and table name properly for quoted SQL access."""
    if schema:
        return f'"{schema}"."{table}"'
    else:
        return f'"{table}"'


def clean_column_name(col):
    """Cleans and normalizes column names for consistent database and DataFrame usage.
    
    Example:
        'D+1' -> 'd_1'
        'Order External ID' -> 'order_external_id'
        'Month' -> 'mnth'
        'Group' -> 'group_name'
    """
    if col is None:
        return None

    col = str(col).lower().strip()
    col = re.sub(r'^(null|none|\s+)+', '', col)
    col = re.sub(r'[ \+\./\\()\[\]{}\-]+', '_', col)

    # Replace specific reserved words
    # if col == 'month':
    #     col = 'mnth'
    if col == 'group':
        col = 'group_name'

    # If column starts with a digit, prefix with 'v'
    if re.match(r'^\d', col):
        col = 'v' + col

    col = re.sub(r'_+', '_', col).strip('_')
    return col
