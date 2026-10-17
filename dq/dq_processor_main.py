import great_expectations as gx  # Use gx for Great Expectations
from great_expectations.core.batch import BatchRequest
import logging
import datetime
from configuration import config, engine, dq_rules_table ,dq_results_table # Import dq_rules_table here
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, distinct
from sqlalchemy import create_engine

# Load the DQ database configuration from config.toml
dq_db_config = config["dq_database"]  # This is for the DQ database
datasource_name = dq_db_config["datasource_name"]
data_connector_name = dq_db_config["data_connector_name"]

# Establish connection to the DQ database using SQLAlchemy
dq_connection_string = f"{dq_db_config['driver']}://{dq_db_config['user']}:{dq_db_config['password']}@{dq_db_config['host']}:{dq_db_config['port']}/{dq_db_config['dbname']}"
dq_engine = create_engine(dq_connection_string)

# Function to configure the datasource programmatically, only if it doesn't exist
def configure_datasource(context, datasource_name, dq_db_config):
    existing_datasources = context.list_datasources()
    
    # Check if the datasource is already present
    if not any(ds['name'] == datasource_name for ds in existing_datasources):
        context.sources.add_sql(
            name=datasource_name,
            connection_string=dq_connection_string,  # Use the dynamically built connection string
        )
        logging.info(f"Datasource '{datasource_name}' added programmatically.")
    else:
        logging.info(f"Datasource '{datasource_name}' already exists. Skipping addition.")

def create_expectation_suite(context, suite_name):
    # Create the expectation suite if it doesn't exist
    try:
        suite = context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
        logging.info(f"Expectation suite '{suite_name}' created successfully.")
        return suite
    except Exception as e:
        logging.error(f"Error while creating expectation suite '{suite_name}': {str(e)}")
        raise

def process_all_tables_dq():
    logger = logging.getLogger(__name__)
    
    # Start a new session
    Session = sessionmaker(bind=engine)  # Using engine for the rules database
    session = Session()

    try:
        # Fetch distinct table names from the dq_rules_table
        distinct_tables_query = select(distinct(dq_rules_table.c.table_name))
        distinct_tables = session.execute(distinct_tables_query).fetchall()

        # Loop through all distinct tables and call the run_dq_at_rest function for each table
        for table_row in distinct_tables:
            table_name = table_row[0]
            logger.info(f"Running DQ checks for table: {table_name}")
            run_dq_at_rest(table_name, logger)

    except Exception as e:
        logger.error(f"Error while processing DQ for tables: {str(e)}")
    finally:
        # Close the session
        session.close()

def run_dq_at_rest(table_name, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)
    logger.info(f"Running DQ at rest for table: {table_name}")

    # Mapping rule types to Great Expectations expectations
    rule_type_to_expectation = {
        "not_null": "expect_column_values_to_not_be_null",
        "unique": "expect_column_values_to_be_unique",
        "range": "expect_column_values_to_be_between",
        "regex": "expect_column_values_to_match_regex",
        "custom": "expect_column_values_to_be_greater_than"
    }

    # Create a session to fetch DQ rules from the rules database
    Session = sessionmaker(bind=engine)
    session = Session()

    # Fetch DQ rules for the given table from the rules database
    rules_query = select(dq_rules_table).where(
        (dq_rules_table.c.table_name == table_name) # Only apply at rest rules
    )
    
    dq_rules = session.execute(rules_query).fetchall()
    logger.info(f"Fetched DQ rules for table {table_name}: {len(dq_rules)} rules found.")

    # Use the Great Expectations Fluent API context
    context = gx.get_context()

    # Configure the datasource programmatically, only if it doesn't exist
    configure_datasource(context, datasource_name, dq_db_config)

    # Create the expectation suite
    suite_name = f"{table_name}_expectations"
    suite = create_expectation_suite(context, suite_name)

    # Create the BatchRequest for the Validator
    batch_request = BatchRequest(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=table_name  # The table name you're validating
    )

    # Create a Validator from the batch request
    try:
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )
    except Exception as e:
        logger.error(f"Error while creating validator for table {table_name}: {str(e)}")
        return

    # Apply DQ rules to the Validator (NOT the suite)
    logger.info(f"Adding expectations to validator for table '{table_name}'...")

    for rule in dq_rules:
        rule_type = rule.rule_type
        rule_expression = rule.rule_expression
        rule_id = rule.rule_id
        column_name = rule.column_name

        logger.info(f"Adding DQ check for rule_id {rule_id} on column {column_name} with rule_type {rule_type}")

        try:
            # Map rules to Great Expectations expectations (on the validator)
            if rule_type == "not_null":
                validator.expect_column_values_to_not_be_null(column_name)
            elif rule_type == "unique":
                validator.expect_column_values_to_be_unique(column_name)
            elif rule_type == "range":
                # Assuming rule_expression is in the form 'BETWEEN min AND max'
                min_value, max_value = map(int, rule_expression.split(' BETWEEN ')[1].split(' AND '))
                validator.expect_column_values_to_be_between(column_name, min_value, max_value)
                logger.info(f"Adding Range check for {column_name} with min value {min_value} and max value {max_value}")
            elif rule_type == "regex":
                # Assuming rule_expression is in the form '~ <regex>'
                regex = rule_expression.split('~')[1].strip()
                validator.expect_column_values_to_match_regex(column_name, regex)
            elif rule_type == "custom":
                # Assuming rule_expression is in the form 'column > value'
                validator.expect_column_values_to_be_greater_than(column_name, int(rule_expression.split('>')[1].strip()))
            else:
                logger.error(f"Unsupported rule_type: {rule_type}")
                continue
        except Exception as e:
            logger.error(f"Error while adding expectation for rule_id {rule_id} on column {column_name}: {str(e)}")
            continue

    logger.info(f"All expectations added for table {table_name}. Proceeding to validation...")

    # Validate the data directly in the DQ database (MySQL or others)
    try:
        validation_result = validator.validate()
        logger.info(f"Validation result: {validation_result}")
    except Exception as e:
        logger.error(f"Error while running validation for table {table_name}: {str(e)}")
        return

    # Log the results in the dq_results_table (assuming dq_results_table is already defined)
    for expectation_result in validation_result["results"]:
        expectation_config = expectation_result["expectation_config"]
        column_name = expectation_config["kwargs"]["column"]
        status = "pass" if expectation_result["success"] else "fail"
        records_passed = expectation_result["result"].get("element_count", 0) - expectation_result["result"].get("unexpected_count", 0)
        records_failed = expectation_result["result"].get("unexpected_count", 0)
        failed_records = expectation_result["result"].get("partial_unexpected_list", [])
        logger.info(f"Column: {column_name}, Passed: {records_passed}, Failed: {records_failed}, Status: {status}")
        
        # Map back the rule_id for the current column
        rule_id = next(
            (rule.rule_id for rule in dq_rules if rule.column_name == column_name and rule_type_to_expectation[rule.rule_type] == expectation_config["expectation_type"]),
            None
        )

        # Insert DQ results into the dq_results_table
        session.execute(dq_results_table.insert().values(
            table_name=table_name,
            column_name=column_name,
            rule_id=rule_id,
            status=status,
            failed_records=str(failed_records),
            checked_at=datetime.datetime.now(),
            records_passed=records_passed,
            records_failed=records_failed
        ))
    session.commit()
    logger.info(f"DQ checks completed for table: {table_name}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    process_all_tables_dq()
