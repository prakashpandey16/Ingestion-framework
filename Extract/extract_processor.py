import logging
import os
import tempfile  
import pandas as pd
from Extract.pdf_extractor import extract_performance_data, extract_invoice_data, extract_airtel_bill_data
import boto3
from utils.db_config_loader import get_db_connection_string, get_aws_access_secret_key
from Extract.select_executor import get_max_watermark_value, construct_select_statement, add_watermark_condition, execute_select, construct_partitioned_select_statement
from decorators.decorators import log_execution_time, handle_errors
import connectorx as cx
import re
import datetime

@handle_errors
@log_execution_time
def extract_data(entry, log_id, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)
    source_db_id = entry['source_db_id']
    target_db_id = entry['target_db_id']
    source_table = entry['source_table']
    qualified_select = entry['qualified_select']
    target_table = entry['target_table']
    watermark_column = entry['watermark_column']
    source_schema = entry['source_schema']
    target_schema = entry['target_schema']

    use_connectorx = entry['use_connectorx']
    source_connection_string, source_db_type = get_db_connection_string(source_db_id, is_source=True)
    target_connection_string, target_db_type = get_db_connection_string(target_db_id, is_source=False)
    # Construct the select statement
    select_statement = construct_select_statement(source_table, qualified_select, source_schema)
    
    
    max_watermark_value=None
    # Add watermark condition if applicable
    if watermark_column:
          max_watermark_value = get_max_watermark_value(target_table, watermark_column, target_connection_string, target_schema)
          if max_watermark_value:
                select_statement = add_watermark_condition(select_statement, watermark_column, max_watermark_value)
                logger.info(f"Max watermark value: {max_watermark_value}")
    
          logger.info(f"Select statement: {select_statement}")
    if use_connectorx:
        # Use ConnectorX to fetch data as DataFrame
        
        if "postgresql" in source_connection_string:
            source_connection_string = source_connection_string.replace("postgresql", "postgres")
        elif "mysql+pymysql" in source_connection_string:
            source_connection_string = source_connection_string.replace("mysql+pymysql", "mysql")
            
        logger.info(f"reading with connector x the Select statement: {select_statement}")
        df = cx.read_sql(source_connection_string, select_statement)
        logger.info(f"read and returned: {select_statement}")
        return df, max_watermark_value
    # Execute the select statement
    data = execute_select(select_statement, source_connection_string)

    return data, max_watermark_value

def extract_distinct_partition_values(entry, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)

    source_db_id = entry['source_db_id']
    source_table = entry['source_table']
    partition_column = entry['chunking_column']
    source_schema = entry['source_schema']

    source_connection_string, source_db_type = get_db_connection_string(source_db_id, is_source=True)

    select_statement = f"SELECT DISTINCT {partition_column} as partition_value from {source_schema}.{source_table}" if source_schema else f"SELECT DISTINCT {partition_column} as partition_value from {source_table}"
    data = execute_select(select_statement, source_connection_string)

    # Convert each row into a consistent format
    return [{'partition_value': row['partition_value']} for row in data]




def extract_distinct_date_partitions(entry, date_column, chunk_size, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)

    source_db_id = entry['source_db_id']
    source_table = entry['source_table']
    source_schema = entry['source_schema']

    source_connection_string, source_db_type = get_db_connection_string(source_db_id, is_source=True)

    if source_db_type == 'postgresql':
        if chunk_size == 1:  # Yearly
            select_statement = f"SELECT DISTINCT EXTRACT(YEAR FROM {date_column})::INTEGER as year_part FROM {source_schema}.{source_table}" if source_schema else f"SELECT DISTINCT EXTRACT(YEAR FROM {date_column})::INTEGER as year_part FROM {source_table}"
        elif chunk_size == 2:  # Monthly
            select_statement = f"SELECT DISTINCT (EXTRACT(YEAR FROM {date_column})::INTEGER || '-' || EXTRACT(MONTH FROM {date_column})::INTEGER) as month_part FROM {source_schema}.{source_table}" if source_schema else f"SELECT DISTINCT (EXTRACT(YEAR FROM {date_column})::INTEGER || '-' || EXTRACT(MONTH FROM {date_column})::INTEGER) as month_part FROM {source_table}"
        elif chunk_size == 3:  # Daily
            select_statement = f"SELECT DISTINCT {date_column} FROM {source_schema}.{source_table}" if source_schema else f"SELECT DISTINCT {date_column} FROM {source_table}"

    elif source_db_type == 'oracle':
        if chunk_size == 1:  # Yearly
            select_statement = f"SELECT DISTINCT TO_CHAR({date_column}, 'YYYY') as year_part FROM {source_schema}.{source_table}" if source_schema else f"SELECT DISTINCT TO_CHAR({date_column}, 'YYYY') as year_part FROM {source_table}"
        elif chunk_size == 2:  # Monthly
            select_statement = f"SELECT DISTINCT TO_CHAR({date_column}, 'YYYY-MM') as month_part FROM {source_schema}.{source_table}" if source_schema else f"SELECT DISTINCT TO_CHAR({date_column}, 'YYYY-MM') as month_part FROM {source_table}"
        elif chunk_size == 3:  # Daily
            select_statement = f"SELECT DISTINCT {date_column} FROM {source_schema}.{source_table}" if source_schema else f"SELECT DISTINCT {date_column} FROM {source_table}"

    elif source_db_type == 'mysql':
        if chunk_size == 1:  # Yearly
            select_statement = f"SELECT DISTINCT YEAR({date_column}) as year_part FROM {source_schema}.{source_table}" if source_schema else f"SELECT DISTINCT YEAR({date_column}) as year_part FROM {source_table}"
        elif chunk_size == 2:  # Monthly
            select_statement = f"SELECT DISTINCT CONCAT(YEAR({date_column}), '-', MONTH({date_column})) as month_part FROM {source_schema}.{source_table}" if source_schema else f"SELECT DISTINCT CONCAT(YEAR({date_column}), '-', MONTH({date_column})) as month_part FROM {source_table}"
        elif chunk_size == 3:  # Daily
            select_statement = f"SELECT DISTINCT {date_column} FROM {source_schema}.{source_table}" if source_schema else f"SELECT DISTINCT {date_column} FROM {source_table}"

    logger.info(f"Extracting distinct date partitions with statement: {select_statement}")
    data = execute_select(select_statement, source_connection_string)

    # Ensure consistent return structure
    return [{'partition_value': str(row['year_part'])} for row in data] if chunk_size == 1 else \
           [{'partition_value': str(row['month_part'])} for row in data] if chunk_size == 2 else \
           [{'partition_value': row[date_column]} for row in data]


def extract_partition_data(entry, partition_value, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)

    source_db_id = entry['source_db_id']
    source_table = entry['source_table']
    target_db_id = entry['target_db_id']
    target_table = entry['target_table']
    watermark_column = entry['watermark_column']
    source_schema = entry['source_schema']
    target_schema = entry['target_schema']
    partition_column = entry['chunking_column']
    chunk_size = entry.get('chunking_size', None)  # Get the chunking size
    use_connectorx = entry['use_connectorx']

    # Get connection strings
    source_connection_string, source_db_type = get_db_connection_string(source_db_id, is_source=True)

    # Construct the select statement using partition information
    select_statement = construct_partitioned_select_statement(
        source_table,
        partition_column,
        partition_value,
        source_schema,
        qualified_select=entry.get('qualified_select', None),
        chunk_size=chunk_size,
        source_db_type=source_db_type
    )

    max_watermark_value = None
    
    if use_connectorx:
        # Use ConnectorX to fetch data as DataFrame
        if "postgresql" in source_connection_string:
            source_connection_string = source_connection_string.replace("postgresql", "postgres")
        elif "mysql+pymysql" in source_connection_string:
            source_connection_string = source_connection_string.replace("mysql+pymysql", "mysql")
            
        logger.info(f"reading with connector x the Select statement: {select_statement}")
        df = cx.read_sql(source_connection_string, select_statement)
        logger.info(f"read and returned: {select_statement}")
        return df, max_watermark_value
    
    # Execute the select statement
    data = execute_select(select_statement, source_connection_string)

    return data, max_watermark_value

# Map target_tables to their extraction functions
PDF_HANDLERS = {
    'edsp_daily_metrics': extract_performance_data,
    'ci_edsp_daily_metrics': extract_performance_data,
    'performance_report': extract_performance_data,
    'invoice': extract_invoice_data,
    'airtel_bill': extract_airtel_bill_data,
}

def extract_data_from_s3(entry,log_id, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)
    if not isinstance(entry, dict) or 'file_type' not in entry or 's3_file_path' not in entry or 'bucket_name' not in entry:
        logger.error(f"Invalid entry format for log_id {log_id}: {entry}")
        raise ValueError("Entry must be a dict with 'file_type', 's3_file_path', and 'bucket_name'.")
    
    source_db_id = entry['source_db_id']
    file_type = entry['file_type'].lower()
    bucket = entry['bucket_name']
    prefix = entry['s3_file_path'].rstrip('/') + '/' if entry['s3_file_path'] else ''
    regex_pattern = entry['regex_pattern']
    order_by = entry['order_by_time'].lower()



    if file_type not in ['csv', 'json', 'txt', 'xlsx', 'pdf']:
        logger.error(f"Unsupported file_type '{file_type}' for log_id {log_id}")
        raise ValueError("Supported file_types: 'csv', 'json', 'txt', 'xls', 'xlsx', 'pdf'.")
    if order_by not in ['asc', 'desc']:
        logger.error(f"Invalid order_by '{order_by}' for log_id {log_id}")
        raise ValueError("order_by must be 'asc' or 'desc'.")
    move_to_ignored = entry.get('move_to_ignored', False)
    if move_to_ignored and 'ignored_prefix' not in entry:
        logger.error(f"missing 'ignored_prefix' when 'move_ignored' is True for log_id {log_id}")
        raise ValueError("If 'move_ignored' is True, 'ignored_prefix' must be provided in entry.")
    try:
        pattern = re.compile(regex_pattern)
    except re.error as e:
        logger.error(f"Invalid regex_pattern '{regex_pattern}' for log_id {log_id}: {str(e)}")
        raise ValueError("Invalid regex pattern.")

    # Initialize local_path to None to prevent UnboundLocalError
    local_path = None 
    
    # Create S3 client
    s3_client = get_s3_client(source_db_id)
    try:
        matching_keys, ignored_keys = get_selected_key(s3_client, bucket, prefix, pattern, order_by, log_id, logger)
        
        if not matching_keys:
             logger.warning(f"No matching files found for regex logic after update for log_id {log_id}")
             # If no files found, we can decide to return empty DF or raise error. 
             # Previous code raised error, let's keep consistency but allow for 0 files if that's valid?
             # User prompt implies they expect files.
             raise ValueError("No matching files found.")

        # Limit to 1 file if not PDF
        if file_type != 'pdf' and len(matching_keys) > 1:
            logger.info(f"File type is '{file_type}'. Limiting processing to 1 file per run. Keeping the first file out of {len(matching_keys)}.")
            matching_keys = [matching_keys[0]]

        all_dfs = []
        processed_keys = []

        # Get the system's temporary directory (works on Windows/Linux)
        temp_dir = tempfile.gettempdir()

        for key in matching_keys:
            local_path = None
            try:
                # Define local path
                file_name = key.split('/')[-1]
                local_path = os.path.join(temp_dir, f"{log_id}_{file_name}")

                logger.info(f"Starting download of {key} from bucket {bucket} for log_id {log_id}")
                s3_client.download_file(bucket, key, local_path)
                logger.info(f"Successfully downloaded to {local_path} for log_id {log_id}")

                # Process the file based on its type
                df_current = pd.DataFrame()
                if file_type == 'csv':
                    df_current = pd.read_csv(local_path)
                elif file_type == 'json':
                    df_current = pd.read_json(local_path, lines=True)  
                elif file_type == 'txt':
                    with open(local_path, 'r') as f:
                        lines = f.readlines()
                    df_current = pd.DataFrame({'text': [line.strip() for line in lines]})
                elif file_type == 'xlsx':
                     # For Excel files, we need to read from a file-like object
                    import io
                    excel_data = open(local_path, 'rb').read()
                    df_current = pd.read_excel(io.BytesIO(excel_data), engine='openpyxl')
                elif file_type == 'pdf':
                    target_table = entry.get('target_table', '')
                    # Use source_handler_key if available, otherwise fallback/empty
                    handler_key = entry.get('source_handler_key')
                    
                    # Check handler by key first, then fallback to target_table for backward compatibility
                    handler = None
                    if handler_key and handler_key in PDF_HANDLERS:
                        handler = PDF_HANDLERS[handler_key]
                    elif target_table in PDF_HANDLERS: # Fallback to target_table matching
                        handler = PDF_HANDLERS[target_table]
                    
                    if handler:
                        extracted_data = handler(local_path)
                        df_current = pd.DataFrame(extracted_data)
                        
                        # Add tracking columns for airtel_bills
                        if handler_key == 'airtel_bill' or target_table == 'airtel_fixedline_bill':
                            secondary_bucket = entry.get('secondary_bucket_name')
                            secondary_prefix = entry.get('secondary_prefix')
                            relative_key = key[len(prefix):] if prefix else key
                            
                            df_current['bucket_name'] = secondary_bucket if secondary_bucket else bucket
                            df_current['folder_name'] = secondary_prefix.rstrip('/') if secondary_prefix else ''
                            df_current['file_name'] = relative_key
                    else:
                        logger.warning(f"No PDF handler defined for handler_key '{handler_key}' or target_table '{target_table}' in log_id {log_id}")
                else:
                    logger.error(f"Unhandled file_type '{file_type}' for log_id {log_id}")
                    raise ValueError(f"Unhandled file_type: {file_type}")
                
                if not df_current.empty:
                    all_dfs.append(df_current)
                
                processed_keys.append(key)

            except Exception as e:
                logger.error(f"Error processing file {key} for log_id {log_id}: {str(e)}")
                # Continue processing other files? Or fail batch?
                # Usually better to fail fast on errors, or log and continue. 
                # Let's re-raise for now to be safe, unless user requests partial success.
                raise 

            finally:
                # Clean up the downloaded file
                try:
                    if local_path and os.path.exists(local_path):
                        os.remove(local_path)
                        logger.info(f"Successfully cleaned up temporary file {local_path}")
                except Exception as e:
                    logger.warning(f"Could not remove temporary file {local_path} for log_id {log_id}: {str(e)}")

        # Combine all dataframes
        if all_dfs:
            df = pd.concat(all_dfs, ignore_index=True)
            logger.info(f"Combined {len(all_dfs)} files into DataFrame with {len(df)} rows.")
        else:
            df = pd.DataFrame()
            logger.warning("No data extracted from any files.")


        # Move processed files
        # Removed the intermediate processed file move here. It is now handled entirely by the
        # weekly move logic inside etl_runner.py after a successful database load.

        if move_to_ignored:
            ignored_prefix = entry['ignored_prefix']
            if ignored_keys:
                 move_s3_objects(s3_client, bucket, ignored_keys, prefix, ignored_prefix, log_id, logger)

        # Return concatenated DF and list of processed keys (or single key string if downstream expects str?)
        # Original signature returned: return df, key
        # We should return a representation of keys processed. 
        # CAUTION: Downstream might expect a string. Let's return comma-separated or first key?
        # Let's return the list of keys as a string representation or similar.
        return df, processed_keys

    except Exception as e:
        logger.error(f"Error processing matching files from S3 for log_id {log_id}: {str(e)}")
        raise

def get_selected_key(s3_client, bucket, prefix, pattern, order_by, log_id, logger):
    """
    Lists objects in the S3 prefix, filters using the compiled regex pattern on relative keys,
    sorts by LastModified timestamp (asc or desc), and returns ALL matching keys.
    
    :param s3_client: boto3 S3 client
    :param bucket: S3 bucket name
    :param prefix: S3 prefix (directory)
    :param pattern: Compiled regex pattern
    :param order_by: 'asc' or 'desc'
    :param log_id: Identifier for logging
    :param logger: Logger object for logging
    :return: list - List of selected S3 keys
    """
    logger.info(f"Listing objects in prefix '{prefix}' for bucket '{bucket}' for log_id {log_id}")
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    matching_files = []
    ignored_keys=[]
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if not key.endswith('/'):  # Exclude directories
                    relative_key = key[len(prefix):] if prefix else key
                    if '/' in relative_key:  # Skip subdirectories
                        continue
                    if pattern.match(relative_key):
                        matching_files.append(obj)
                    else:
                        ignored_keys.append(key)
    
    if not matching_files:
        logger.warning(f"No matching files found for regex '{pattern.pattern}' in prefix '{prefix}' for log_id {log_id}")
        # Return empty list instead of raising error to allow graceful exit or logging upstream
        return [], ignored_keys
    
    # Sort by LastModified
    reverse = (order_by == 'desc')  # desc means newest first
    matching_files.sort(key=lambda x: x['LastModified'], reverse=reverse)
    
    # Return all matching keys
    matching_keys = [obj['Key'] for obj in matching_files]
    logger.info(f"Found {len(matching_keys)} matching files: {matching_keys}")

    return matching_keys, ignored_keys
def move_s3_objects(s3_client, bucket, keys, source_prefix, target_prefix, log_id, logger):
    """
    Moves S3 objects to the target_prefix.
    
    *** MODIFIED: Strips existing 'processed_' prefixes and timestamps before 
    adding new ones to avoid duplicate tagging. ***
    """
    if not isinstance(keys, list):
        keys = [keys]
        
    moved_files = []
    
    for key in keys:
        try:
            # Get the filename (e.g., "uploads/processed_file_2025-11-05_100000.csv" -> "processed_file_2025-11-05_100000.csv")
            relative_key = key[len(source_prefix):] if source_prefix else key
            
            # Split extension (e.g., root="processed_file_2025-11-05_100000", ext=".csv")
            filename_root, filename_ext = os.path.splitext(relative_key)
            
            # --- CLEANING LOGIC START ---
            # 1. Remove existing 'processed_' prefix if present
            if filename_root.startswith('processed_'):
                filename_root = filename_root[10:] # len('processed_') is 10

            # 2. Remove existing timestamp suffix if present (regex for _YYYY-MM-DD_HHMMSS)
            # This regex looks for the timestamp pattern at the very end of the filename root
            filename_root = re.sub(r'_\d{4}-\d{2}-\d{2}_\d{6}$', '', filename_root)
            # --- CLEANING LOGIC END ---

            # 3. Generate new tags
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
            
            # 4. Create new clean filename
            # Result: processed_[original_name]_[new_timestamp].ext
            unique_relative_key = f"processed_{filename_root}_{timestamp}{filename_ext}"
            
            new_key = target_prefix.rstrip('/') + '/' + unique_relative_key

            logger.info(f"Moving key '{key}' to '{new_key}' for log_id {log_id}")
            
            copy_source = {'Bucket': bucket, 'Key': key}
            s3_client.copy_object(CopySource=copy_source, Bucket=bucket, Key=new_key)
            s3_client.delete_object(Bucket=bucket, Key=key)
            
            # --- Extract invoice number ---
            invoice_number = None
            inv_match = re.search(r'(INV-[A-Z0-9]+-\d+)', filename_root)
            if inv_match:
                invoice_number = inv_match.group(1)
            elif 'INV-' in filename_root:
                inv_match_fallback = re.search(r'(INV-[a-zA-Z0-9\-]+)', filename_root)
                if inv_match_fallback:
                    invoice_number = inv_match_fallback.group(1).split('_')[0]
                    
            moved_files.append({
                'bucket_name': bucket,
                'folder_name': target_prefix.rstrip('/'),
                'file_name': unique_relative_key,
                'invoice_number': invoice_number
            })
            
        except Exception as e:
            logger.error(f"Error moving key '{key}' for log_id {log_id}: {str(e)}")

    return moved_files

def copy_s3_objects(s3_client, source_bucket, target_bucket, keys, source_prefix, target_prefix, log_id, logger):
    """
    Copies S3 objects to a target_bucket and target_prefix without deleting the source.
    """
    if not isinstance(keys, list):
        keys = [keys]
        
    copied_files = []
    
    for key in keys:
        try:
            relative_key = key[len(source_prefix):] if source_prefix else key
            
            new_key = target_prefix.rstrip('/') + '/' + relative_key

            logger.info(f"Copying key '{key}' from bucket '{source_bucket}' to '{new_key}' in bucket '{target_bucket}' for log_id {log_id}")
            
            copy_source = {'Bucket': source_bucket, 'Key': key}
            s3_client.copy_object(CopySource=copy_source, Bucket=target_bucket, Key=new_key)
            
            copied_files.append({
                'bucket_name': target_bucket,
                'folder_name': target_prefix.rstrip('/'),
                'file_name': relative_key
            })
            
        except Exception as e:
            logger.error(f"Error copying key '{key}' for log_id {log_id}: {str(e)}")

    return copied_files

def get_s3_client(source_db_id):
    """
    Fetches AWS credentials and creates an S3 client.
    
    :return: boto3 S3 client
    """
    access_key, secret_key = get_aws_access_secret_key(source_db_id,is_source=True)

    return boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )