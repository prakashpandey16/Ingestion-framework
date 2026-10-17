INSERT INTO etl_config.batch_config_table (batch_id,frequency,start_time,active,batch_name,batch_description) VALUES
	 (1,'high','16:41:00',true,NULL,NULL);
INSERT INTO etl_config.dq_rules_table (table_name,column_name,rule_type,rule_expression,apply_in_motion,apply_at_rest) VALUES
	 ('mediumratins','average_rating','range','average_rating  BETWEEN 3 AND 5',true,false),
	 ('mediumratins','tconst','unique','COUNT(tconst) = COUNT(DISTINCT tconst)',true,false);
INSERT INTO etl_config.metadata_table (batch_id,source_db_id,target_db_id,source_table,qualified_select,source_full_select,target_table,watermark_column,watermark_value,load_type,business_key,use_hash,delete_handling,delete_table_name,source_schema,target_schema,column_operations_flag,column_operations_key,dq_in_motion,dq_at_rest,active) VALUES
	 (1,6,4,'title_ratings','select * from title_ratings where numvotes <5000  and average_rating >8',NULL,'mediumratins',NULL,'','scd2','tconst,average_rating',false,NULL,NULL,'public',NULL,false,NULL,true,false,true),
	 (1,6,4,'customers','select * from jaffle_shop.customers',NULL,'jaffle_customers',NULL,'','scd2','id,first_name,last_name',false,NULL,NULL,'jaffle_shop',NULL,false,NULL,false,false,true),
	 (1,7,3,'TestScan','',NULL,'createTestScan',NULL,'','append','',false,NULL,NULL,NULL,'ingestion',false,NULL,false,false,true),
	 (1,6,4,'orders','select * from jaffle_shop.orders',NULL,'orders',NULL,'','truncate','',false,NULL,NULL,'jaffle_shop',NULL,false,NULL,false,false,true);
INSERT INTO etl_config.source_db_config_table (source_db_name,connection_string,host_ip,port,username,"password",driver,"database") VALUES
	 ('mylocalpostgresDB','postgresql://priyankchalana:Smile%40123@localhost:5432/mydb','localhost',5432,'priyankchalana','Smile%40123','postgresql','mydb'),
	 ('mysqldathub_db','mysql+pymysql://datahub:datahub@localhost:3306/datahub','localhost',3306,'datahub','datahub','mysql+pymysql','datahub');
INSERT INTO etl_config.target_db_config_table (target_db_name,connection_string,host_ip,port,username,"password",driver,"database") VALUES
	 ('mylocaltargetpostgres','postgresql://priyankchalana:Smile%40123@localhost:5432/mydb','localhost',5432,'priyankchalana','Smile%40123','postgresql','mydb'),
	 ('mylocaltargetmysql','mysql+pymysql://root:datahub@localhost:3306/New_DB','localhost',3306,'root','datahub','mysql+pymysql','New_DB');


/* To test transformations*/
/* Test incremental loads */
/* logging issues for worker log generation on filesystem*/
/* Email notification in case of errors */