"""
Data Quality Processor Module

Provides DQ validation using Great Expectations for both in-motion and at-rest data.
"""
import logging
import datetime
import great_expectations as ge
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
from conf.config import engine, dq_rules_table, dq_results_table


# Mapping from rule types to Great Expectations expectation methods
RULE_TYPE_TO_EXPECTATION = {
    "not_null": "expect_column_values_to_not_be_null",
    "unique": "expect_column_values_to_be_unique",
    "range": "expect_column_values_to_be_between",
    "regex": "expect_column_values_to_match_regex",
    "custom": "expect_column_values_to_be_greater_than"
}


def _run_dq_validation(data, table_name: str, in_motion: bool, logger=None):
    """
    Unified DQ validation logic for both in-motion and at-rest data.
    
    Args:
        data: Input data (DataFrame or list of dicts)
        table_name: Name of the table for rule lookup
        in_motion: True for in-motion validation, False for at-rest
        logger: Optional logger instance
        
    Returns:
        Original data format (DataFrame if input was DataFrame, else dict)
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Fetch applicable rules based on in_motion flag
        rules_query = select(dq_rules_table).where(
            (dq_rules_table.c.table_name == table_name) &
            (dq_rules_table.c.apply_in_motion == in_motion)
        )
        dq_rules = session.execute(rules_query).fetchall()

        # Track if input was DataFrame for return type consistency
        is_dataframe = isinstance(data, pd.DataFrame)
        if not is_dataframe:
            data = pd.DataFrame(data)

        # Create Great Expectations dataset
        df = ge.dataset.PandasDataset(data)
        logger.info(f"DQ check starting - Rows: {len(df)}, Mode: {'in-motion' if in_motion else 'at-rest'}")

        # Apply each rule as an expectation
        for rule in dq_rules:
            rule_type = rule.rule_type
            rule_expression = rule.rule_expression
            rule_id = rule.rule_id
            column_name = rule.column_name

            logger.info(f"Applying DQ rule {rule_id}: {rule_type} on column '{column_name}'")

            if rule_type == "not_null":
                df.expect_column_values_to_not_be_null(column_name)
            elif rule_type == "unique":
                df.expect_column_values_to_be_unique(column_name)
            elif rule_type == "range":
                min_value, max_value = map(int, rule_expression.split(' BETWEEN ')[1].split(' AND '))
                df.expect_column_values_to_be_between(column_name, min_value, max_value)
                logger.info(f"Range check: {column_name} between {min_value} and {max_value}")
            elif rule_type == "regex":
                regex = rule_expression.split('~')[1].strip()
                df.expect_column_values_to_match_regex(column_name, regex)
            elif rule_type == "custom":
                df.expect_column_values_to_be_greater_than(column_name, int(rule_expression.split('>')[1].strip()))
            else:
                logger.error(f"Unsupported rule_type: {rule_type}")
                continue

        # Run validation
        validation_result = df.validate()
        logger.info(f"Validation completed - Success: {validation_result['success']}")

        # Process and log results
        for expectation_result in validation_result["results"]:
            expectation_config = expectation_result["expectation_config"]
            column_name = expectation_config["kwargs"]["column"]
            status = "pass" if expectation_result["success"] else "fail"
            records_passed = expectation_result["result"].get("element_count", 0) - expectation_result["result"].get("unexpected_count", 0)
            records_failed = expectation_result["result"].get("unexpected_count", 0)
            failed_records = expectation_result["result"].get("partial_unexpected_list", [])
            expectation_type = expectation_config["expectation_type"]

            # Find matching rule_id
            rule_id = next(
                (rule.rule_id for rule in dq_rules 
                 if rule.column_name == column_name and RULE_TYPE_TO_EXPECTATION.get(rule.rule_type) == expectation_type),
                None
            )

            logger.info(f"DQ Result - Rule {rule_id}: {status} | Passed: {records_passed}, Failed: {records_failed}")

            # Store results in database
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

        # Return in original format
        if is_dataframe:
            return data
        else:
            return data.to_dict(orient='records')

    finally:
        session.close()


def run_dq_in_motion(data, table_name, logger=None):
    """
    Runs data quality checks for in-motion (streaming/pipeline) data.
    
    Args:
        data: Input data (DataFrame or list of dicts)
        table_name: Name of the table for rule lookup
        logger: Optional logger instance
        
    Returns:
        Original data format
    """
    return _run_dq_validation(data, table_name, in_motion=True, logger=logger)


def run_dq_at_rest(data, table_name, logger=None):
    """
    Runs data quality checks for at-rest (batch/stored) data.
    
    Args:
        data: Input data (DataFrame or list of dicts)
        table_name: Name of the table for rule lookup
        logger: Optional logger instance
        
    Returns:
        Original data format
    """
    return _run_dq_validation(data, table_name, in_motion=False, logger=logger)
