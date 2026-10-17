import logging
import pandas as pd
from conf.metadata_loader import fetch_metadata
from Extract.extract_processor import extract_data, extract_data_from_s3, extract_distinct_partition_values, extract_partition_data, extract_distinct_date_partitions, get_s3_client, move_s3_objects, copy_s3_objects
from Transform.transform_executor import transform_data
from Load.load_processor import load_data
from utils.db_config_loader import get_db_connection_string
from load_handlers.scd2_loader import SCD2Loader
from load_handlers.append_loader import AppendLoader
from load_handlers.truncate_loader import TruncateLoader
from decorators.decorators import log_execution_time, handle_errors
from utils.schema_manager import ensure_table_in_target_for_files, ensure_target_table_exists
from utils.logging_utils import log_batch_start, log_error_details, log_audit_details, send_failure_notification
from sqlalchemy.orm import sessionmaker
import datetime
from conf.config import engine
from recon.recon_processor import fetch_recon_entries, process_recon_entry
from utils.mail_notifier import send_success_mail, send_failure_mail

logger = logging.getLogger(__name__)


@handle_errors
@log_execution_time
def process_metadata_entry(entry, log_id, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)

    column_operations_flag = entry['column_operations_flag']
    column_operations_key = entry.get('column_operations_key', None)
    is_large_table = entry['is_large_table']
    chunking_method = entry.get('chunking_method', None)
    chunking_column = entry.get('chunking_column', None)
    chunk_size = entry.get('chunking_size', None)
    file_flag = entry['file_flag']
    file_flag = entry['file_flag']

    # Ensure we have job_name and batch_id for emailer (safe fallbacks)
    batch_id = entry.get("batch_id") or datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    job_name = f"{entry['target_table']}_{batch_id}"

    result = {
        'records_read': 0,
        'records_written': 0,
        'watermark_value': None,
        'process_time': datetime.datetime.now(),
        'inserts': 0,
        'updates': 0,
        'deletes': 0,
        'source_table': entry['source_table'],
        'target_table': entry['target_table'],
        'start_time': datetime.datetime.now(),
        'end_time': None,
        'duration': None,
        'status': 'in_progress',
        'metadata_id': entry['id'],
        'source_db_id': entry['source_db_id'],
        'target_db_id': entry['target_db_id']
    }

    Session = sessionmaker(bind=engine)
    session = Session()
    max_watermark_value = None

    try:
        # Pre-ETL Recon
        logger.info("Fetching pre-ETL recon entries (if any)")
        pre_recon_entries = fetch_recon_entries(entry['id'], time_to_run='pre')
        for recon_entry in pre_recon_entries:
            process_recon_entry(recon_entry, log_id)


        # Standard Pipelines
        log_audit_details(session, log_id, {entry['id']: result})

        # --- CHUNKING ---
        if is_large_table and chunking_method:
            partition_values = []
            if chunking_method == "By a Date column":
                partition_values = extract_distinct_date_partitions(entry, chunking_column, chunk_size, logger)
            else:
                partition_values = extract_distinct_partition_values(entry, logger)

            insert_count = update_count = delete_count = 0
            data = pd.DataFrame()
            ensure_target_table_exists(entry, data, logger)

            for record in partition_values:
                value = record['partition_value']
                data, max_watermark_value = extract_partition_data(entry, value, logger)
                if column_operations_flag and column_operations_key:
                    data = transform_data(data, column_operations_key, logger)
                ic, uc, dc = load_data(entry, data, log_id, logger)
                insert_count += ic
                update_count += uc
                delete_count += dc

            result.update({
                'records_read': len(data),
                'records_written': insert_count + update_count,
                'watermark_value': max_watermark_value,
                'inserts': insert_count,
                'updates': update_count,
                'deletes': delete_count,
                'status': 'success'
            })

        else:
            # NON-CHUNKED FLOW
            if file_flag:
                data, selected_key = extract_data_from_s3(entry, log_id, logger)
            else:
                data, max_watermark_value = extract_data(entry, log_id, logger)

            if column_operations_flag and column_operations_key:
                data = transform_data(data, column_operations_key, logger)

            ensure_target_table_exists(entry, data, logger)
            insert_count, update_count, delete_count = load_data(entry, data, log_id, logger)

            # WEEKLY MOVE
            if file_flag and selected_key:
                source_db_id = entry['source_db_id']
                source_prefix = entry['s3_file_path'].rstrip('/') + '/'
                bucket = entry['bucket_name']
                base_processed_prefix = entry['processed_prefix'].rstrip('/')

                today = datetime.datetime.now()
                # Calculate start (Monday) and end (Sunday) of the current week
                start_of_week = today - datetime.timedelta(days=today.weekday())
                end_of_week = start_of_week + datetime.timedelta(days=6)
                week_folder_name = f"processed_{start_of_week.strftime('%Y-%m-%d')}_to_{end_of_week.strftime('%Y-%m-%d')}"
                processed_prefix = f"{base_processed_prefix}/{week_folder_name}/"

                s3_client = get_s3_client(source_db_id)
                
                # S3 extraction sometimes returns a string list "['file']". Safely parse it.
                import ast
                if isinstance(selected_key, str) and selected_key.startswith('['):
                    try:
                        selected_key_parsed = ast.literal_eval(selected_key)
                    except:
                        selected_key_parsed = [selected_key]
                else:
                    selected_key_parsed = selected_key

                # Optional secondary copy
                secondary_bucket = entry.get('secondary_bucket_name')
                secondary_prefix = entry.get('secondary_prefix')
                
                if secondary_bucket and secondary_prefix:
                    copy_s3_objects(
                        s3_client=s3_client,
                        source_bucket=bucket,
                        target_bucket=secondary_bucket,
                        keys=selected_key_parsed,
                        source_prefix=source_prefix,
                        target_prefix=secondary_prefix,
                        log_id=log_id,
                        logger=logger
                    )

                moved_files = move_s3_objects(
                    s3_client=s3_client,
                    bucket=bucket,
                    keys=selected_key_parsed,
                    source_prefix=source_prefix,
                    target_prefix=processed_prefix,
                    log_id=log_id,
                    logger=logger
                )
                
                if moved_files:
                    # Only store S3 movement info in the DB if the workflow is for invoices
                    is_invoice = (entry.get('source_handler_key') == 'invoice' or entry.get('target_table') == 'invoice' or entry.get('target_table') == 'ci_amazon_revenue')
                    if is_invoice:
                        from conf.config import processed_files_log_table
                        from sqlalchemy import select as sa_select
                        for moved_file in moved_files:
                            invoice_num = moved_file.get('invoice_number')
                            
                            # Dedup: If the same invoice was already processed, update
                            # the existing log entry instead of creating a duplicate.
                            # This handles re-runs where the same PDF is processed again.
                            existing_entry = None
                            if invoice_num:
                                existing_entry = session.execute(
                                    sa_select(processed_files_log_table).where(
                                        processed_files_log_table.c.invoice_number == invoice_num
                                    )
                                ).fetchone()
                            
                            if existing_entry:
                                # Update existing entry with the latest file path
                                logger.info(f"Invoice {invoice_num} already in log — updating existing entry.")
                                update_stmt = (
                                    processed_files_log_table.update()
                                    .where(processed_files_log_table.c.invoice_number == invoice_num)
                                    .values(
                                        log_id=log_id,
                                        metadata_id=entry['id'],
                                        bucket_name=moved_file['bucket_name'],
                                        folder_name=moved_file['folder_name'],
                                        file_name=moved_file['file_name'],
                                        processed_at=datetime.datetime.now()
                                    )
                                )
                                session.execute(update_stmt)
                            else:
                                # First time processing — insert new entry
                                insert_stmt = processed_files_log_table.insert().values(
                                    log_id=log_id,
                                    metadata_id=entry['id'],
                                    bucket_name=moved_file['bucket_name'],
                                    folder_name=moved_file['folder_name'],
                                    file_name=moved_file['file_name'],
                                    invoice_number=invoice_num,
                                    processed_at=datetime.datetime.now()
                                )
                                session.execute(insert_stmt)
                        session.commit()


            result.update({
                'job_name': job_name,
                'records_read': len(data),
                'records_written': insert_count + update_count,
                'watermark_value': max_watermark_value,
                'inserts': insert_count,
                'updates': update_count,
                'deletes': delete_count,
                'status': 'success'
            })

        # --- POST-ETL RECON ---    
        post_recon_entries = fetch_recon_entries(entry['id'], time_to_run='post')
        for recon_entry in post_recon_entries:
            process_recon_entry(recon_entry, log_id)

        # email notification (consolidated - removed individual success mail)
        # Success emails are now sent as a batch summary in main loop
        result['status'] = 'success'

    except Exception as e:
        result['status'] = 'failure'

        # Log error for audit system
        log_error_details(session, log_id, entry['id'], str(e), logger)

        # Removed individual send_failure_mail, now handled at the batch level in main_batch_run.py / main.py
        
        raise e

    finally:
        result['end_time'] = datetime.datetime.now()
        result['duration'] = (result['end_time'] - result['start_time']).total_seconds()
        log_audit_details(session, log_id, {entry['id']: result})
        session.close()

    return result
