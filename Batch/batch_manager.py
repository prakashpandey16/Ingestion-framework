import logging
import datetime
import multiprocessing
import os
from sqlalchemy.orm import sessionmaker
from Batch.log_manager import setup_logging_for_batch
from utils.logging_utils import log_batch_start, log_batch_end
from conf.config import engine
from Batch.worker_manager import worker_run

def run_batch_wrapper(batch, log_dir):
    logger = logging.getLogger('main')
    logger.info(f"Starting run_batch_wrapper for batch {batch.batch_id}")
    run_batch(batch, log_dir)
    logger.info(f"Finished run_batch_wrapper for batch {batch.batch_id}")

def run_batch(batch, log_dir):
    Session = sessionmaker(bind=engine)
    session = Session()
    logger = setup_logging_for_batch(batch.batch_id, log_dir)
    process_id = os.getpid()
    logger.info(f"Starting run_batch with PID {process_id} and batch {batch.batch_id}")

    run_start = datetime.datetime.now()
    log_id = None
    try:
        log_id = log_batch_start(session, batch.batch_id, run_start)
        logger.info(f"Batch {batch.batch_id} started with log_id {log_id}")
        
        from conf.metadata_loader import fetch_metadata
        metadata = fetch_metadata(batch.batch_id)
        tasks = [(worker_id, entry, log_id, log_dir) for worker_id, entry in enumerate(metadata)]
        
        # Log the metadata assignment
        for worker_id, entry in enumerate(metadata):
            logger.info(f"Assigning metadata entry {entry['id']} to worker {worker_id} at {datetime.datetime.now()}")

        with multiprocessing.get_context("spawn").Pool(len(tasks)) as pool:
            results = pool.map(worker_run, tasks)

        logger.info(f"Batch {batch.batch_id} processing completed")
        success_count = results.count('success')
        failure_count = results.count('failure')

        run_end = datetime.datetime.now()
        log_batch_end(session, log_id, run_end, 'success', len(metadata), success_count, failure_count)
        logger.info(f"Batch {batch.batch_id} ended successfully")

    except Exception as e:
        if log_id is not None:
            log_batch_end(session, log_id, datetime.datetime.now(), 'failure', len(metadata), 0, len(metadata))
        logger.error(f"Error during batch processing: {e}")
        raise

    finally:
        session.close()
