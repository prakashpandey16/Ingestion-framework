import logging
import os
from sqlalchemy.orm import sessionmaker
from Batch.log_manager import setup_logging_for_worker
from conf.config import engine

def worker_run(args):
    worker_id, metadata_entry, log_id, log_dir = args
    logger = setup_logging_for_worker(worker_id, log_dir)
    logger.info(f"Worker {worker_id} starting task for metadata entry {metadata_entry['id']}")

    Session = sessionmaker(bind=engine)
    session = Session()

    status = 'failure'
    try:
        from etl_runner import process_metadata_entry
        process_metadata_entry(metadata_entry, log_id, logger)
        status = 'success'
        logger.info(f"Worker {worker_id} successfully processed metadata entry {metadata_entry['id']}")
    except Exception as e:
        logger.error(f"Error during metadata processing for worker {worker_id}: {e}")
    finally:
        session.close()

    return status
