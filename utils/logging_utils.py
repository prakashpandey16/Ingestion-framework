from sqlalchemy import text
from sqlalchemy.exc import OperationalError, DisconnectionError, TimeoutError
import logging
import datetime
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from conf.config import config as app_config

logger = logging.getLogger(__name__)

# Read retry settings from config
_etl_config = app_config.get("etl", {})
DEFAULT_LOG_MAX_RETRIES = _etl_config.get("log_max_retries", 3)
DEFAULT_LOG_RETRY_DELAY = _etl_config.get("log_retry_delay_seconds", 5)


def log_batch_start(session, batch_id, run_start):
    """
    Inserts a new record into the batch run log to mark the start of an ETL batch.

    Args:
        session: The SQLAlchemy session object.
        batch_id: The ID of the batch being run.
        run_start: The start timestamp of the run.

    Returns:
        The newly created log_id.
    """
    sql = text("INSERT INTO etl_config.batch_run_log_table (batch_id, run_start, status) VALUES (:batch_id, :run_start, 'inprogress') RETURNING log_id")
    result = session.execute(sql, {'batch_id': batch_id, 'run_start': run_start})
    log_id = result.scalar()
    session.commit()
    return log_id


def log_batch_end(session, log_id, end_time, status, metadata_entries_run=0, metadata_entries_succeeded=0, metadata_entries_failed=0, max_retries=None, initial_delay=None):
    """
    Updates the batch run log with status details, including a retry mechanism.
    Retry settings read from config.toml if not specified.
    """
    if max_retries is None:
        max_retries = DEFAULT_LOG_MAX_RETRIES
    if initial_delay is None:
        initial_delay = DEFAULT_LOG_RETRY_DELAY
    sql = text("""
        UPDATE etl_config.batch_run_log_table
        SET run_end = :run_end,
            status = :status,
            metadata_entries_run = :metadata_entries_run,
            metadata_entries_succeeded = :metadata_entries_succeeded,
            metadata_entries_failed = :metadata_entries_failed
        WHERE log_id = :log_id
    """)

    for attempt in range(max_retries):
        try:
            session.execute(sql, {
                'run_end': end_time,
                'status': status,
                'metadata_entries_run': metadata_entries_run,
                'metadata_entries_succeeded': metadata_entries_succeeded,
                'metadata_entries_failed': metadata_entries_failed,
                'log_id': log_id
            })
            session.commit()
            return  # Exit the function on success

        except (OperationalError, DisconnectionError, TimeoutError) as e:
            if attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt)  # Exponential backoff
                logger.warning(f"Database error occurred: {e}. Retrying in {delay} seconds...")
                session.rollback()  # Rollback the transaction to reset the session state
                time.sleep(delay)
            else:
                logger.error(f"Failed to log batch end after {max_retries} attempts.")
                session.rollback()  # Final rollback
                raise  # Re-raise the exception after all retries are exhausted


def log_audit_details(session, log_id, results):
    """
    Logs detailed audit information for each metadata entry processed in a batch.

    Args:
        session: The SQLAlchemy session object.
        log_id: The ID of the batch log.
        results: A dictionary containing audit details for each metadata entry.
    """
    try:
        for metadata_id, details in results.items():
            sql = text("""
                INSERT INTO etl_config.batch_audit_log_table
                (log_id, metadata_id, table_name, records_read, records_written, watermark_value, process_time, inserts, updates, deletes, source_db_id, target_db_id, status, start_time, end_time, duration)
                VALUES (:log_id, :metadata_id, :table_name, :records_read, :records_written, :watermark_value, :process_time, :inserts, :updates, :deletes, :source_db_id, :target_db_id, :status, :start_time, :end_time, :duration)
                ON CONFLICT (log_id, metadata_id) DO UPDATE
                SET
                    records_read = EXCLUDED.records_read,
                    records_written = EXCLUDED.records_written,
                    watermark_value = EXCLUDED.watermark_value,
                    process_time = EXCLUDED.process_time,
                    inserts = EXCLUDED.inserts,
                    updates = EXCLUDED.updates,
                    deletes = EXCLUDED.deletes,
                    source_db_id = EXCLUDED.source_db_id,
                    target_db_id = EXCLUDED.target_db_id,
                    status = EXCLUDED.status,
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    duration = EXCLUDED.duration
            """)
            session.execute(sql, {
                'log_id': log_id,
                'metadata_id': metadata_id,
                'table_name': details['target_table'],
                'records_read': details['records_read'],
                'records_written': details['records_written'],
                'watermark_value': details['watermark_value'],
                'process_time': details['process_time'],
                'inserts': details['inserts'],
                'updates': details['updates'],
                'deletes': details['deletes'],
                'source_db_id': details['source_db_id'],
                'target_db_id': details['target_db_id'],
                'status': details['status'],
                'start_time': details['start_time'],
                'end_time': details['end_time'],
                'duration': details['duration']
            })
            session.commit()
    except Exception as e:
        logger.error(f"Error logging audit details: {e}")
        session.rollback()


def log_audit_details_backup(session, log_id, results):
    """
    A backup/alternative function to log audit details. It lacks ON CONFLICT functionality.

    Args:
        session: The SQLAlchemy session object.
        log_id: The ID of the batch log.
        results: A dictionary containing audit details for each metadata entry.
    """
    for table_name, details in results.items():
        if not isinstance(details, dict):
            logging.error(f"Expected details to be a dict but got {type(details)}: {details}")
            continue
        sql = text("""
            INSERT INTO etl_config.batch_audit_log_table (
                log_id, table_name, records_read, records_written, 
                watermark_value, process_time, inserts, updates, 
                deletes, metadata_id, source_db_id, target_db_id, 
                start_time, end_time, duration, status
            ) VALUES (
                :log_id, :table_name, :records_read, :records_written, 
                :watermark_value, :process_time, :inserts, :updates, 
                :deletes, :metadata_id, :source_db_id, :target_db_id, 
                :start_time, :end_time, :duration, :status
            )
        """)
        session.execute(sql, {
            'log_id': log_id,
            'table_name': table_name,
            'records_read': details['records_read'],
            'records_written': details['records_written'],
            'watermark_value': details['watermark_value'],
            'process_time': details['process_time'],
            'inserts': details['inserts'],
            'updates': details['updates'],
            'deletes': details['deletes'],
            'metadata_id': details['metadata_id'],
            'source_db_id': details['source_db_id'],
            'target_db_id': details['target_db_id'],
            'start_time': details['start_time'],
            'end_time': details['end_time'],
            'duration': details['duration'],
            'status': details['status']
        })
    session.commit()


def log_error_details(session, log_id, metadata_id, error_message, logger):
    """
    Logs specific error messages related to a single metadata entry.

    Args:
        session: The SQLAlchemy session object.
        log_id: The ID of the batch log.
        metadata_id: The ID of the metadata entry that failed.
        error_message: The error message string.
        logger: The logging object.
    """
    try:
        sql = text("""
            INSERT INTO etl_config.error_log_table
            (log_id, metadata_id, error_message, error_time)
            VALUES (:log_id, :metadata_id, :error_message, :error_time)
        """)
    
        session.execute(sql, {
            'log_id': log_id,
            'metadata_id': metadata_id,
            'error_message': error_message,
            'error_time': datetime.datetime.now()
        })
        session.commit()
    except Exception as e:
        logger.error(f"Error logging error details: {e}")
        session.rollback()


def send_failure_notification(subject, body):
    """
    Sends an email notification upon a batch processing failure.

    Args:
        subject: The subject of the email.
        body: The body content of the email.
    """
    config = {
        "email": {
            "smtp_server": "smtp.gmail.com",
            "smtp_port": 587,
            "smtp_user": "chalana.priyank@gmail.com",
            "smtp_password": "Smile@123",
            "recipient": "nebercanezor@gmail.com"
        }
    }
    
    try:
        msg = MIMEMultipart()
        msg['From'] = config['email']['smtp_user']
        msg['To'] = config['email']['recipient']
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(config['email']['smtp_server'], config['email']['smtp_port'])
        server.starttls()
        server.login(config['email']['smtp_user'], config['email']['smtp_password'])
        text = msg.as_string()
        server.sendmail(config['email']['smtp_user'], config['email']['recipient'], text)
        server.quit()
        logger.info(f"Failure notification sent to {config['email']['recipient']}")
    except Exception as e:
        logger.error(f"Failed to send email notification: {e}")