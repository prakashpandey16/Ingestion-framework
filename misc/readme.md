

### ETL Framework Summary

This ETL (Extract, Transform, Load) framework is designed to provide a scalable and flexible solution for managing and processing data from various source databases to target databases. The framework includes configuration-driven metadata management, dynamic data extraction, transformation, and loading capabilities, as well as robust logging, auditing, and error handling mechanisms.

#### Key Features

- ### `Configuration-Driven`
  - Centralized configuration management using a `config.toml` file.
  - Source and target database configurations stored in dedicated tables (`source_db_config_table`, `target_db_config_table`).
  - Metadata-driven batch processing, allowing dynamic adjustments without code changes (`metadata_table`, `batch_config_table`).

- ### `Extract Phase`
  - Dynamic or pre Qualified SQL SELECT statement construction and execution based on metadata.
  - Support for watermark-based incremental data extraction.

- ### `Transform Phase`
  - Column transformations including adding, dropping, and converting columns based on enumerated types.
  - Configurable transformations stored in `column_operations_table` and `enumerated_types_table`.

- ### `Load Phase`
  - Multiple loading strategies supported: Slowly Changing Dimension Type 2 (SCD2), Append, and Truncate.
  - Ensures target table existence and structure before loading.
  - Integration with the `dlt` library for alternative loading mechanisms.

- ### `Batch Processing`
  - Scheduled batch processing with configurable frequencies.
  - Worker management for parallel processing, utilizing multiple CPU cores.
  - Detailed logging and auditing for batch runs and individual table operations.

- ### `Logging and Auditing`
  - Comprehensive logging system with separate log files for each worker.
  - Audit logs capturing records read, written, and transformation details.
  - **Audit Tables**:
    - `batch_run_log_table`: Logs the start and end times of each batch run, along with the status (`success`, `failure`).
    - `batch_audit_log_table`: Logs details for each table processed in a batch, including records read, records written, watermark value, inserts, updates, and deletes.
  - This increases the observability of the solution, providing detailed insights into the ETL process.


- ### `Error Handling and Notifications`
  - Decorators for logging execution time and handling errors.
  - Decorators for Retry mechanisms and failure notifications (planned for future enhancements).

- ### `Streamlit Frontend`
  - User-friendly Streamlit app to manage configurations and view audit-log details.
  - Allows adding, updating, and deleting entries in configuration tables.
  - Enables viewing detailed logs of batch runs and audit logs for easy monitoring and troubleshooting.


## Project Structure



project/  
│
├── main.py  
├── conf/  
│   ├── config.py   
│   ├── metadata_loader.py  
├── batch/  
│   ├── batch_scheduler.py  
│   ├── worker_manager.py  
│   ├── log_manager.py  
├── extract/  
│   ├── select_executor.py  
│   ├── extract_processor.py  
├── transform/  
│   ├── transform_executor.py  
│   ├── transform_processor.py  
├── load/  
│   ├── load_processor.py  
├── utils/
│   ├── table_utils.py  
│   ├── logging_utils.py  
│   ├── db_config_loader.py  
├── load_handlers/  
│   ├── base_loader.py  
│   ├── scd2_loader.py  
│   ├── append_loader.py  
│   ├── truncate_loader.py  
│   ├── dlt_loader.py  
│
└── config.toml  


## Files Description and Important Functions

### `conf/`

**config.py**
- Defines the configuration settings and database connection details.
- Creates the SQLAlchemy engine and metadata.
- **Important Functions**:
  - `create_engine()`
  - `MetaData()`
  - `Table()`

**metadata_loader.py**
- Fetches metadata for each batch job.
- **Important Functions**:
  - `fetch_metadata(batch_id)`

### `batch/`

**batch_scheduler.py**
- Schedules batch jobs based on the configuration.
- **Important Functions**:
  - `schedule_batches()`
  - `run_batch(batch_id)`

**worker_manager.py**
- Manages the worker processes.
- **Important Functions**:
  - `start_workers()`
  - `assign_batches_to_workers(batches)`

**log_manager.py**
- Manages the log files and directories.
- **Important Functions**:
  - `setup_logging()`
  - `cleanup_old_logs()`

### `extract/`

**select_executor.py**
- Contains functions for executing SQL SELECT statements and handling watermark conditions.
- **Important Functions**:
  - `add_watermark_condition(select_statement, watermark_column, watermark_value)`
  - `get_max_watermark_value(target_table, watermark_column, connection_string, target_schema)`
  - `execute_select(statement, connection_string)`
  - `construct_select_statement(source_table, qualified_select, source_schema)`

**extract_processor.py**
- Processes the metadata entries for the extraction phase.
- **Important Functions**:
  - `extract_data(entry, log_id, logger)`

### `transform/`

**transform_executor.py**
- Applies column transformations to the data.
- **Important Functions**:
  - `apply_enum_conversion(session, data, source_column, target_column, enum_id)`
  - `transform_data(data, column_operations_key, logger)`

**transform_processor.py**
- Processes the metadata entries for the transformation phase.
- **Important Functions**:
  - `transform_data(data, column_operations_key, logger)`

### `load/`

**load_processor.py**
- Processes the metadata entries for the load phase.
- **Important Functions**:
  - `load_data(entry, data, log_id, logger)`

### `utils/`

**table_utils.py**
- Ensures the target table exists and performs necessary operations.
- **Important Functions**:
  - `ensure_target_table_exists(source_table_name, target_table_name, source_connection_string, target_connection_string, source_schema, target_schema, load_type)`

**logging_utils.py**
- Manages logging for batch processes.
- **Important Functions**:
  - `log_batch_start(session, batch_id, run_start)`
  - `log_batch_end(session, log_id, run_end, status)`
  - `log_audit_details(session, log_id, results)`

**db_config_loader.py**
- Loads database connection configurations.
- **Important Functions**:
  - `get_db_connection_string(db_id, is_source)`

### `load_handlers/`

**base_loader.py**
- Base class for different load types.
- **Important Functions**:
  - `__init__(self, target_table, connection_string, target_schema, load_type)`
  - `load_data(self, data)`
  - `close(self)`

**scd2_loader.py**
- Handles Slowly Changing Dimension Type 2 (SCD2) loads.
- **Important Functions**:
  - `get_business_key_hash(self, record)`
  - `handle_inserts_updates(self, data)`
  - `handle_deletes(self, data=None, source_keys=None, delete_table_name=None)`
  - `load_data(self, data, source_keys=None, delete_table_name=None)`

**append_loader.py**
- Handles append-only loads.
- **Important Functions**:
  - `load_data(self, data)`

**truncate_loader.py**
- Handles truncate and load operations.
- **Important Functions**:
  - `load_data(self, data)`

**dlt_loader.py**
- Handles loads using the dlt library.
- **Important Functions**:
  - `load_data_with_dlt(self, data)`


  #### Future Enhancements

- **Performance Improvements**:
  - Optimize the ETL process to handle larger datasets efficiently.
  - Implement performance monitoring and tuning mechanisms.

- **Support for Additional Engines**:
  - Integrate other engines apart from SQLAlchemy for the load phase, such as ConnectorX and pandas or pyarrow.

- **Support for Different Source and Target Types**:
  - Extend support to include various file formats (CSV, Parquet, etc.) as both source and target.
  - Add support for different data sources such as cloud storage, NoSQL databases, etc.

- **Data Encryption**:
  - Implement support for encryption of data at rest and in transit to ensure data security and compliance.


