-- DROP SCHEMA etl_config;

CREATE SCHEMA etl_config AUTHORIZATION priyankchalana;

-- DROP SEQUENCE etl_config.batch_run_log_table_log_id_seq;

CREATE SEQUENCE etl_config.batch_run_log_table_log_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE etl_config.column_operations_table_id_seq;

CREATE SEQUENCE etl_config.column_operations_table_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE etl_config.dq_results_table_id_seq;

CREATE SEQUENCE etl_config.dq_results_table_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE etl_config.dq_rules_table_rule_id_seq;

CREATE SEQUENCE etl_config.dq_rules_table_rule_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE etl_config.enumerated_types_table_id_seq;

CREATE SEQUENCE etl_config.enumerated_types_table_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE etl_config.error_log_table_error_id_seq;

CREATE SEQUENCE etl_config.error_log_table_error_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE etl_config.metadata_table_id_seq;

CREATE SEQUENCE etl_config.metadata_table_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE etl_config.source_db_config_table_id_seq;

CREATE SEQUENCE etl_config.source_db_config_table_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE etl_config.target_db_config_table_id_seq;

CREATE SEQUENCE etl_config.target_db_config_table_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;-- etl_config.batch_audit_log_table definition

-- Drop table

-- DROP TABLE etl_config.batch_audit_log_table;

CREATE TABLE etl_config.batch_audit_log_table (
	log_id int4 NOT NULL,
	metadata_id int4 NOT NULL,
	table_name varchar(255) NOT NULL,
	records_read int4 NULL,
	records_written int4 NULL,
	watermark_value varchar(255) NULL,
	process_time timestamp NULL,
	inserts int4 NULL,
	updates int4 NULL,
	deletes int4 NULL,
	source_db_id int4 NULL,
	target_db_id int4 NULL,
	status varchar(15) NULL,
	start_time timestamp NULL,
	end_time timestamp NULL,
	duration float8 NULL,
	CONSTRAINT batch_audit_log_table_pkey PRIMARY KEY (log_id, metadata_id)
);


-- etl_config.batch_config_table definition

-- Drop table

-- DROP TABLE etl_config.batch_config_table;

CREATE TABLE etl_config.batch_config_table (
	batch_id int4 NOT NULL,
	frequency varchar(10) NOT NULL,
	start_time time NOT NULL,
	active bool DEFAULT true NULL,
	batch_name varchar(255) NULL,
	batch_description text NULL,
	CONSTRAINT batch_config_table_pkey PRIMARY KEY (batch_id)
);


-- etl_config.column_operations_table definition

-- Drop table

-- DROP TABLE etl_config.column_operations_table;

CREATE TABLE etl_config.column_operations_table (
	id serial4 NOT NULL,
	target_table varchar NOT NULL,
	column_name varchar NOT NULL,
	operation_type varchar NOT NULL,
	sql_syntax text NULL,
	derived_method varchar NULL,
	enum_id int4 NULL,
	source_column varchar NULL,
	CONSTRAINT column_operations_table_pkey PRIMARY KEY (id)
);


-- etl_config.dq_rules_table definition

-- Drop table

-- DROP TABLE etl_config.dq_rules_table;

CREATE TABLE etl_config.dq_rules_table (
	rule_id serial4 NOT NULL,
	table_name varchar(255) NOT NULL,
	column_name varchar(255) NULL,
	rule_type varchar(255) NOT NULL,
	rule_expression text NOT NULL,
	apply_in_motion bool DEFAULT false NULL,
	apply_at_rest bool DEFAULT false NULL,
	CONSTRAINT dq_rules_table_pkey PRIMARY KEY (rule_id)
);


-- etl_config.enumerated_types_table definition

-- Drop table

-- DROP TABLE etl_config.enumerated_types_table;

CREATE TABLE etl_config.enumerated_types_table (
	id serial4 NOT NULL,
	enum_id int4 NOT NULL,
	enum_code varchar NOT NULL,
	enum_value varchar NOT NULL,
	CONSTRAINT enumerated_types_table_pkey PRIMARY KEY (id)
);


-- etl_config.source_db_config_table definition

-- Drop table

-- DROP TABLE etl_config.source_db_config_table;

CREATE TABLE etl_config.source_db_config_table (
	id serial4 NOT NULL,
	source_db_name varchar(255) NOT NULL,
	connection_string text NOT NULL,
	host_ip varchar(255) NOT NULL,
	port int4 NOT NULL,
	username varchar(255) NOT NULL,
	"password" varchar(255) NOT NULL,
	driver varchar(255) NOT NULL,
	"database" varchar(255) NOT NULL,
	CONSTRAINT source_db_config_table_pkey PRIMARY KEY (id)
);


-- etl_config.target_db_config_table definition

-- Drop table

-- DROP TABLE etl_config.target_db_config_table;

CREATE TABLE etl_config.target_db_config_table (
	id serial4 NOT NULL,
	target_db_name varchar(255) NOT NULL,
	connection_string text NOT NULL,
	host_ip varchar(255) NOT NULL,
	port int4 NOT NULL,
	username varchar(255) NOT NULL,
	"password" varchar(255) NOT NULL,
	driver varchar(255) NOT NULL,
	"database" varchar(255) NOT NULL,
	CONSTRAINT target_db_config_table_pkey PRIMARY KEY (id)
);


-- etl_config.batch_run_log_table definition

-- Drop table

-- DROP TABLE etl_config.batch_run_log_table;

CREATE TABLE etl_config.batch_run_log_table (
	log_id serial4 NOT NULL,
	batch_id int4 NOT NULL,
	run_start timestamp NOT NULL,
	run_end timestamp NULL,
	status varchar(15) NOT NULL,
	metadata_entries_run int4 NULL,
	metadata_entries_succeeded int4 NULL,
	metadata_entries_failed int4 NULL,
	CONSTRAINT batch_run_log_table_pkey PRIMARY KEY (log_id),
	CONSTRAINT batch_run_log_table_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES etl_config.batch_config_table(batch_id)
);


-- etl_config.metadata_table definition

-- Drop table

-- DROP TABLE etl_config.metadata_table;

CREATE TABLE etl_config.metadata_table (
	id serial4 NOT NULL,
	batch_id int4 NOT NULL,
	source_db_id int4 NOT NULL,
	target_db_id int4 NOT NULL,
	source_table varchar(255) NOT NULL,
	qualified_select text NULL,
	source_full_select text NULL,
	target_table varchar(255) NOT NULL,
	watermark_column varchar(255) NULL,
	watermark_value varchar(255) NULL,
	load_type varchar(50) NOT NULL,
	business_key varchar(255) NULL,
	use_hash bool DEFAULT false NULL,
	delete_handling varchar(20) NULL,
	delete_table_name varchar(255) NULL,
	source_schema varchar(255) NULL,
	target_schema varchar(255) NULL,
	column_operations_flag bool DEFAULT false NULL,
	column_operations_key int4 NULL,
	dq_in_motion bool DEFAULT false NULL,
	dq_at_rest bool DEFAULT false NULL,
	active bool DEFAULT true NULL,
	CONSTRAINT metadata_table_pkey PRIMARY KEY (id),
	CONSTRAINT metadata_table_source_db_id_fkey FOREIGN KEY (source_db_id) REFERENCES etl_config.source_db_config_table(id),
	CONSTRAINT metadata_table_target_db_id_fkey FOREIGN KEY (target_db_id) REFERENCES etl_config.target_db_config_table(id)
);


-- etl_config.error_log_table definition

-- Drop table

-- DROP TABLE etl_config.error_log_table;

CREATE TABLE etl_config.error_log_table (
	error_id serial4 NOT NULL,
	log_id int4 NOT NULL,
	metadata_id int4 NOT NULL,
	error_message text NULL,
	log_file_location varchar(255) NULL,
	error_time timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT error_log_table_pkey PRIMARY KEY (error_id),
	CONSTRAINT error_log_table_log_id_fkey FOREIGN KEY (log_id) REFERENCES etl_config.batch_run_log_table(log_id),
	CONSTRAINT error_log_table_metadata_id_fkey FOREIGN KEY (metadata_id) REFERENCES etl_config.metadata_table(id)
);


-- etl_config.dq_results_table definition

-- Drop table

-- DROP TABLE etl_config.dq_results_table;

CREATE TABLE etl_config.dq_results_table (
	id serial4 NOT NULL,
	table_name varchar(255) NOT NULL,
	column_name varchar(255) NULL,
	rule_id int4 NULL,
	status varchar(255) NOT NULL,
	failed_records text NULL,
	checked_at timestamp NOT NULL,
	records_passed int4 NULL,
	records_failed int4 NULL,
	CONSTRAINT dq_results_table_pkey PRIMARY KEY (id)
);


-- etl_config.dq_results_table foreign keys

ALTER TABLE etl_config.dq_results_table ADD CONSTRAINT dq_results_table_rule_id_fkey FOREIGN KEY (rule_id) REFERENCES ingestion.dq_rules_table(rule_id);


-- etl_config.audit_log definition
-- Audit log table for web scraper runs

-- DROP TABLE etl_config.audit_log;

CREATE TABLE etl_config.audit_log (
	id serial4 NOT NULL,
	amazon_id int4 NOT NULL,
	station varchar(50) NOT NULL,
	report_name varchar(255) NOT NULL,
	status varchar(50) NOT NULL,
	time_taken float8 NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT audit_log_pkey PRIMARY KEY (id),
	CONSTRAINT audit_log_amazon_id_fkey FOREIGN KEY (amazon_id) REFERENCES etl_config.amazon_config(id)
);