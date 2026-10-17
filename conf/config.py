from email.policy import default
import toml
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey, Time, Boolean , Text, Float, DateTime, DECIMAL, JSON
from datetime import datetime

# Load the configuration file
config = toml.load("config.toml")

# Create the database connection string
db_config = config["database"]
aws_config = config.get("aws", {})
connection_string = f"{db_config['driver']}://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

# Create the engine and metadata
schema_name = db_config.get('schema', 'public')  # Default schema is 'public'

import os
# Execution Mode Configuration
# 'SERVICE': Runs health check server (default for backward compatibility if not specified)
# 'BATCH': Strict batch execution, no server, strict exit codes
execution_mode = os.environ.get('EXECUTION_MODE', 'SERVICE').upper()




import logging
logger = logging.getLogger(__name__)

class EngineManager:
    """
    Singleton-like manager for database engines to ensure connection pooling
    and prevent excessive engine creation churn.
    """
    _engines = {}

    @classmethod
    def get_engine(cls, connection_string, db_type=None):
        if connection_string in cls._engines:
            return cls._engines[connection_string]

        # Configure pooling options
        pool_args = {
            'pool_size': 5,
            'max_overflow': 2,
            'pool_recycle': 1800,
            'pool_pre_ping': True
        }
        
        if 'sqlite' in connection_string: 
             new_engine = create_engine(connection_string)
        else:
             new_engine = create_engine(connection_string, **pool_args)
        
        if new_engine:
            logger.info(f"Creating NEW engine for connection: {connection_string.split('@')[-1]}")
             
        cls._engines[connection_string] = new_engine
        return new_engine

# Create the engine and metadata using EngineManager
engine = EngineManager.get_engine(connection_string)
metadata = MetaData(schema=schema_name)

# Define the tables
source_db_config_table = Table(
    'source_db_config_table', metadata,
    Column('id', Integer, primary_key=True),
    Column('source_db_name', String, nullable=False),
    Column('db_type',String,nullable=False),
    Column('connection_string', String, nullable=False),
    Column('s3_access_key', String, nullable=False),
    Column('s3_secret_key', String, nullable=False)


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
    Column('chunking_method', String),# New column for chunking method
    Column('file_flag', Boolean, default=False),
    Column('bucket_name', String, nullable=False),
    Column('s3_file_path', String, nullable=False),
    Column('file_type', String, nullable=False),
    Column('regex_pattern', String, nullable=False),
    Column('order_by_time', String, nullable=False),
    Column('move_to_ignored', Boolean, default=False),
    Column('processed_prefix', String, nullable=False),
    Column('ignored_prefix', String, nullable=False),
    Column('source_handler_key', String),
    Column('secondary_bucket_name', String),
    Column('secondary_prefix', String)





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

processed_files_log_table = Table(
    'processed_files_log_table', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('log_id', Integer, ForeignKey('batch_run_log_table.log_id')),
    Column('metadata_id', Integer, ForeignKey('metadata_table.id')),
    Column('bucket_name', String, nullable=False),
    Column('folder_name', String, nullable=False),
    Column('file_name', String, nullable=False),
    Column('invoice_number', String),
    Column('processed_at', DateTime, default=datetime.now)
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
    Column('records_failed', Integer) ,
    extend_existing=True 
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
amazon_config = Table(
    'amazon_config',
    metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('station', String, nullable=False),
    Column('report_name', String, nullable=False),
    Column('asset_name', String, nullable=False),
    Column('is_active', Boolean),
    Column('created_at', DateTime, server_default='now()'),
    Column('created_by', String, nullable=False),
    Column('user_data_dir', String(255)),
    Column('storage_state', String(255)),
    Column('s3_bucket', String(100)),
    Column('s3_folder', String(255)),
    Column('webscraper_details_id', Integer),
    Column('source_db_id', Integer),
    Column('weeks_offset', Integer),
    Column('file_type', String(50)),
    Column('download_method', String(50))


)
webscraper_details = Table(
    'webscraper_details',metadata,
    Column('id', Integer, primary_key=True),
    Column('url_link', String(255)),
    Column('user_name', String(100)),
    Column('password', String(100)),
    Column('zip_password', String(100))
)

# Airtel Fixedline & Broadband Bill Table
airtel_fixedline_bill_table = Table(
    'airtel_fixedline_bill', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    # Customer Identity
    Column('billing_name', String(255), nullable=False),
    Column('billing_address', Text),
    Column('pincode', String(10)),
    Column('state_code', String(5)),
    Column('place_of_supply', String(100)),
    # Account Details
    Column('fixedline_number', String(20)),
    Column('broadband_id', String(50)),
    Column('relationship_number', String(20)),
    Column('bill_number', String(30), nullable=False, unique=True),
    Column('bill_date', String),
    Column('bill_period_start', String),
    Column('bill_period_end', String),
    Column('pay_by_date', String(30)),
    Column('credit_limit', DECIMAL(12, 2)),
    Column('security_deposit', DECIMAL(12, 2)),
    Column('gst_uin_number', String(20)),
    Column('alternate_mobile_no', String(15)),
    # Account Summary
    Column('previous_balance', DECIMAL(12, 2)),
    Column('payments', DECIMAL(12, 2)),
    Column('this_month_charges', DECIMAL(12, 2)),
    Column('amount_due', DECIMAL(12, 2)),
    Column('due_date', String(30)),
    # Charge Breakdown
    Column('rentals', DECIMAL(12, 2)),
    Column('taxes', DECIMAL(12, 2)),
    Column('total_charges', DECIMAL(12, 2)),
    Column('rental_from_date', String(15)),
    Column('rental_to_date', String(15)),
    # Metadata
    Column('source_file_name', String(255)),
    Column('bucket_name', String(255)),
    Column('folder_name', String(255)),
    Column('file_name', String(255)),
    Column('processed_at', DateTime, default=datetime.now),
)

# Create all tables
metadata.create_all(engine)
