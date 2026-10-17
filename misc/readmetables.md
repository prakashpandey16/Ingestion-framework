# ETL Project

This ETL (Extract, Transform, Load) project is designed to manage the extraction of data from source databases, transformation and watermarking of data, and loading into target databases. The project is modular, with each component handling a specific part of the ETL process.



## Configuration Tables

### 1. `source_db_config_table`

**Purpose**: To store the configuration details of the source databases.

**DDL**:
`sql`
CREATE TABLE source_db_config_table (
    id SERIAL PRIMARY KEY,
    source_db_name VARCHAR(255) NOT NULL,
    connection_string TEXT NOT NULL
);

**SampleData**:
INSERT INTO source_db_config_table (source_db_name, connection_string) VALUES
('SourceDB1', 'postgresql://user:password@localhost:5432/sourcedb1'),
('SourceDB2', 'mysql://user:password@localhost:3306/sourcedb2');


### 2. `target_db_config_table`

**Purpose**: To store the configuration details of the target databases.

**DDL**:
`sql`
CREATE TABLE target_db_config_table (
    id SERIAL PRIMARY KEY,
    target_db_name VARCHAR(255) NOT NULL,
    connection_string TEXT NOT NULL
);

**SampleData**:
INSERT INTO target_db_config_table (target_db_name, connection_string) VALUES
('TargetDB1', 'postgresql://user:password@localhost:5432/targetdb1'),
('TargetDB2', 'mysql://user:password@localhost:3306/targetdb2');


### 3. `metadata_table`

**Purpose**: To store metadata configurations for each batch, including source and target tables, select queries, load types, and business keys.

**DDL**:
`sql`
CREATE TABLE metadata_table (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    source_db_id INTEGER NOT NULL,
    target_db_id INTEGER NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    qualified_select TEXT,
    source_full_select TEXT,
    target_table VARCHAR(255) NOT NULL,
    watermark_column VARCHAR(255),
    watermark_value VARCHAR(255),
    load_type VARCHAR(50) NOT NULL,  -- 'scd2', 'append', 'truncate'
    business_key VARCHAR(255),       -- Comma-separated business key columns
    use_hash BOOLEAN DEFAULT FALSE,
    delete_handling VARCHAR(20),     -- 'feed', 'full_scan', 'separate_table'
    delete_table_name VARCHAR(255),
    FOREIGN KEY (source_db_id) REFERENCES source_db_config_table(id),
    FOREIGN KEY (target_db_id) REFERENCES target_db_config_table(id)
);
**SampleData**:
INSERT INTO metadata_table (batch_id, source_db_id, target_db_id, source_table, qualified_select, source_full_select, target_table, watermark_column, watermark_value, load_type, business_key, use_hash, delete_handling, delete_table_name) VALUES
(1, 1, 1, 'source_table1', 'SELECT * FROM source_table1', NULL, 'target_table1', 'watermark_column1', NULL, 'scd2', 'business_key1', FALSE, 'feed', NULL),
(2, 2, 2, 'source_table2', NULL, 'source_table2', 'target_table2', 'watermark_column2', NULL, 'append', 'business_key2', FALSE, 'full_scan', NULL);


### 4. `batch_config_table`

**Purpose**: To store the configuration details for batch scheduling.

**DDL**:
`sql`
CREATE TABLE batch_config_table (
    batch_id INTEGER PRIMARY KEY,
    frequency VARCHAR(10) NOT NULL,  -- 'high', 'medium', 'low'
    start_time TIME NOT NULL         -- Time of day when the batch should start running
);

**SampleData**:
INSERT INTO batch_config_table (batch_id, frequency, start_time) VALUES
(1, 'high', '00:00:00'),
(2, 'medium', '01:00:00'),
(3, 'low', '00:00:00');



### 5. `batch_run_log_table`

**Purpose**: To store logs of batch runs, including start and end times and status

**DDL**:
`sql`
CREATE TABLE batch_run_log_table (
    log_id SERIAL PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    run_start TIMESTAMP NOT NULL,
    run_end TIMESTAMP,
    status VARCHAR(15) NOT NULL,    -- Example values: 'success', 'failure'
    FOREIGN KEY (batch_id) REFERENCES batch_config_table(batch_id)
);


**SampleData**:
INSERT INTO batch_run_log_table (batch_id, run_start, run_end, status) VALUES
(1, '2024-07-01 00:00:00', '2024-07-01 00:05:00', 'success'),
(2, '2024-07-01 01:00:00', '2024-07-01 01:10:00', 'failure');



### 6. `batch_audit_log_table`

**Purpose**: To store detailed audit logs for each batch run, including record counts and watermark values.

**DDL**:
`sql`
CREATE TABLE batch_audit_log_table (
    audit_id SERIAL PRIMARY KEY,
    log_id INTEGER NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    records_read INTEGER,
    records_written INTEGER,
    watermark_value VARCHAR(255),
    process_time TIMESTAMP,
    inserts INTEGER DEFAULT 0,
    updates INTEGER DEFAULT 0,
    deletes INTEGER DEFAULT 0,
    FOREIGN KEY (log_id) REFERENCES batch_run_log_table(log_id)
);


**SampleData**:
INSERT INTO batch_audit_log_table (log_id, table_name, records_read, records_written, watermark_value, process_time, inserts, updates, deletes) VALUES
(1, 'target_table1', 100, 100, '2024-06-30', '2024-07-01 00:05:00', 100, 0, 0),
(2, 'target_table2', 200, 200, '2024-06-30', '2024-07-01 01:10:00', 0, 200, 0);
