import toml
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey, Time, Boolean , Text, Float, DateTime, DECIMAL, JSON
from datetime import datetime

# Load the configuration file
config = toml.load("config.toml")

# Create the database connection string
db_config = config["database"]
connection_string = f"{db_config['driver']}://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

# Create the engine and metadata
schema_name = db_config.get('schema', 'public')  # Default schema is 'public'

# Create the engine and metadata
print(f"Came here to create the engine {connection_string}")
engine = create_engine(connection_string)
metadata = MetaData(schema=schema_name)

# Define the tables
source_db_config_table = Table(
    'source_db_config_table', metadata,
    Column('id', Integer, primary_key=True),
    Column('source_db_name', String, nullable=False),
    Column('db_type',String,nullable=False),
    Column('connection_string', String, nullable=False)
)

target_db_config_table = Table(
    'target_db_config_table', metadata,
    Column('id', Integer, primary_key=True),
    Column('target_db_name', String, nullable=False),
    Column('db_type',String,nullable=False),
    Column('connection_string', String, nullable=False)
)

metadata_table = Table(
    'metadata_table', metadata,
    Column('id', Integer, primary_key=True),
    Column('batch_id', Integer, nullable=False),
    Column('source_db_id', Integer, ForeignKey('source_db_config_table.id'), nullable=False),
    Column('target_db_id', Integer, ForeignKey('target_db_config_table.id'), nullable=False),
    Column('source_table', String, nullable=False),
    Column('qualified_select', String),
    Column('source_full_select', String),
    Column('target_table', String, nullable=False),
    Column('watermark_column', String),
    Column('watermark_value', String),
    Column('load_type', String, nullable=False),  # 'scd2', 'append', 'truncate'
    Column('business_key', String),  # Comma-separated business key columns
    Column('use_hash', Boolean, default=False),
    Column('delete_handling', String),
    Column('delete_table_name', String),
    Column('source_schema', String),              # Source schema
    Column('target_schema', String),              # Target schema
    Column('column_operations_flag', Boolean, default=False),
    Column('column_operations_key', Integer),
    Column('dq_in_motion', Boolean, default=False),
    Column('dq_at_rest', Boolean, default=False),
    Column('use_connectorx', Boolean, default=False),  # New flag to use ConnectorX
    Column('active', Boolean, default=True),
    Column('is_large_table', Boolean, default=True),
    Column('chunking_size', Integer),  # Renamed from chunking_size_mb
    Column('chunking_column', String),  # Renamed from partition_column
    Column('chunking_method', String)   # New column for chunking method
)

column_operations_table = Table(
    'column_operations_table', metadata,
    Column('id', Integer, primary_key=True),
    Column('target_table', String, nullable=False),
    Column('column_name', String, nullable=False),
    Column('operation_type', String, nullable=False),  # 'add' or 'drop'
    Column('sql_syntax', Text),
    Column('derived_method', String),
    Column('enum_id', Integer),
    Column('source_column', String)
)

# Add the new table definition
enumerated_types_table = Table(
    'enumerated_types_table', metadata,
    Column('id', Integer, primary_key=True),
    Column('enum_id', Integer, nullable=False),
    Column('enum_code', String, nullable=False),
    Column('enum_value', String, nullable=False)
)



batch_config_table = Table(
    'batch_config_table', metadata,
    Column('batch_id', Integer, primary_key=True),
    Column('frequency', String(10), nullable=False),  # 'high', 'medium', 'low'
    Column('start_time', Time, nullable=False),        # Time of day when the batch should start running
    Column('active', Boolean, default=True),
    Column('batch_name', String, nullable=False),
    Column('batch_description', Text, nullable=True)
)


batch_run_log_table = Table(
    'batch_run_log_table', metadata,
    Column('log_id', Integer, primary_key=True, autoincrement=True),
    Column('batch_id', Integer, ForeignKey('batch_config_table.batch_id'), nullable=False),
    Column('run_start', String, nullable=False),
    Column('run_end', String),
    Column('status', String(15), nullable=False),  # 'success', 'failure', 'inprogress'
    Column('metadata_entries_run', Integer),
    Column('metadata_entries_succeeded', Integer),
    Column('metadata_entries_failed', Integer)
)

batch_audit_log_table = Table(
    'batch_audit_log_table', metadata,
    Column('audit_id', Integer, primary_key=True, autoincrement=True),
    Column('log_id', Integer, ForeignKey('batch_run_log_table.log_id'), nullable=False),
    Column('metadata_id', Integer, ForeignKey('metadata_table.id'), nullable=False),
    Column('source_db_id', Integer),
    Column('target_db_id', Integer),
    Column('table_name', String(255), nullable=False),
    Column('records_read', Integer),
    Column('records_written', Integer),
    Column('watermark_value', String(255)),
    Column('process_time', String),
    Column('inserts', Integer),
    Column('updates', Integer),
    Column('deletes', Integer),
    Column('source_table', String(255)),
    Column('target_table', String(255)),
    Column('start_time', String),
    Column('end_time', String),
    Column('duration', Float),
    Column('status', String(15)) # 'success', 'failure', 'inprogress'
)

error_log_table = Table(
    'error_log_table', metadata,
    Column('error_id', Integer, primary_key=True, autoincrement=True),
    Column('log_id', Integer, ForeignKey('batch_run_log_table.log_id'), nullable=False),
    Column('metadata_id', Integer, ForeignKey('metadata_table.id'), nullable=False),
    Column('error_message', Text, nullable=False),
    Column('log_file_location', String(255)),
    Column('error_time', String, nullable=False)
)



dq_rules_table = Table(
    'dq_rules_table', metadata,
    Column('rule_id', Integer, primary_key=True),
    Column('table_name', String, nullable=False),
    Column('column_name', String),
    Column('rule_type', String, nullable=False),  # e.g., 'not_null', 'unique', 'range'
    Column('rule_expression', Text, nullable=False),
    Column('apply_in_motion', Boolean, default=False),
    Column('apply_at_rest', Boolean, default=False)
)

dq_results_table = Table(
    'dq_results_table', metadata,
    Column('id', Integer, primary_key=True),
    Column('table_name', String, nullable=False),
    Column('column_name', String),
    Column('rule_id', Integer, ForeignKey('dq_rules_table.rule_id'), nullable=False),
    Column('status', String, nullable=False),  # e.g., 'pass', 'fail'
    Column('failed_records', Text),  # JSON or serialized format of failed records
    Column('checked_at', String, nullable=False),  # Timestamp of when the DQ check was performed
    Column('records_passed', Integer) ,
    Column('records_failed', Integer)  
)



# Assuming engine and metadata are already created as in your existing config.py

recon_config_table = Table(
    'recon_config', metadata,
    Column('recon_id', Integer, primary_key=True),
    Column('metadata_id', Integer, nullable=False),
    Column('source_db_id', Integer, nullable=False),
    Column('target_db_id', Integer, nullable=False),
    Column('source_table_name', String(255), nullable=False),
    Column('target_table_name', String(255), nullable=False),
    Column('check_type', String(50), nullable=False),  # Options: 'row_count', 'aggregate_check', 'distinct_count', 'sample_check'
    Column('column_name', String(255)),  # Applicable for column-specific checks like 'aggregate_check'
    Column('group_by_column', String(255)),  # Applicable for group-based checks like 'distinct_count'
    Column('aggregation_function', String(50)),  # Options: 'sum', 'avg', 'count', etc.
    Column('sampling_strategy', String(50)),  # Options: 'simple_random', 'systematic', 'stratified', etc.
    Column('sample_size', Integer),  # Sample size for sample checks
    Column('qualifying_column', String(255)),  # Applicable for sampling-based checks
    Column('time_to_run', String(50)),  # Options: 'pre_etl', 'post_etl', 'both'
    Column('run_frequency', String(50)),  # Options: 'daily', 'weekly', 'monthly'
    Column('execution_order', Integer),  # Order in which checks should be run
    Column('is_active', Boolean, default=True),
    Column('created_at', DateTime, default=datetime.now),
    Column('updated_at', DateTime, default=datetime.now, onupdate=datetime.now)
)

recon_results_table = Table(
    'recon_results', metadata,
    Column('recon_result_id', Integer, primary_key=True),
    Column('recon_id', Integer, nullable=False),
    Column('run_timestamp', DateTime, default=datetime.now),
    Column('source_or_target', String(10), nullable=False),  # 'source' or 'target'
    Column('check_type', String(50), nullable=False),  # To keep track of what type of check is being performed
    Column('data_point_value', DECIMAL),  # For numeric values (e.g., row counts, aggregated values)
    Column('data_point_details', JSON),  # For complex details (e.g., hash values, sample row comparison)
    Column('status', String(20), default='pending'),  # 'success', 'failure', 'pending'
    Column('remarks', Text),
    Column('created_at', DateTime, default=datetime.now)
)


# Create all tables
metadata.create_all(engine)
