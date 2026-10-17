import logging
logging.getLogger("great_expectations").setLevel(logging.WARNING)
import multiprocessing
import sys
import sched
import time
import os
import datetime
import threading
from sqlalchemy.orm import sessionmaker
from conf.config import config, engine
from Batch.log_manager import setup_logging
from Batch.batch_scheduler import schedule_batches
from utils.logging_utils import log_batch_start, log_batch_end
from conf.metadata_loader import fetch_metadata
from etl_runner import process_metadata_entry
from queue import Empty
from multiprocessing import Queue
from utils.mail_notifier import send_batch_summary_email
# Load the configuration file
from dotenv import load_dotenv
load_dotenv()

# Enforce BATCH runs for this script
os.environ['EXECUTION_MODE'] = 'BATCH'

# Resolve dynamic paths from environment variables, fallback to config.toml
log_dir = os.environ.get("LOG_DIR", config["etl"]["log_dir"])

def main():
    base_worker_id = config["etl"].get("base_worker_id", 89)  # From config.toml
    run_log_dir = log_dir
    setup_logging(base_worker_id, run_log_dir)
    logger = logging.getLogger('main')

    batch_id = sys.argv[1]
    logger.info("Starting ETL process as base worker")
    
    # Run batch and capture exit code
    exit_code = run_batch(batch_id, run_log_dir, logger)
    
    logger.info(f"Exiting with code {exit_code}")
    sys.exit(exit_code)
    
def run_batch(batch_id, run_log_dir,logger):
    Session = sessionmaker(bind=engine)
    session = Session()
    process_id = os.getpid()
    logger.info(f"Starting run_batch with PID {process_id} and batch {batch_id}")

    run_start = datetime.datetime.now()
    log_id = None
    try:
        log_id = log_batch_start(session, batch_id, run_start)
        logger.info(f"Batch {batch_id} started with log_id {log_id}")
        
        metadata = fetch_metadata(batch_id)
        
        # Create a queue to collect results from workers
        result_queue = multiprocessing.Queue()
        
        tasks = [(worker_id, entry, log_id, run_log_dir, result_queue) for worker_id, entry in enumerate(metadata)]
        
        logger.info(f"Batch {batch_id} assigned {tasks}")
        
        processes = []
        for task in tasks:
            p = multiprocessing.Process(target=worker_run, args=(task,))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

        logger.info(f"Batch {batch_id} processing completed")
        
        # Collect results from queue FIRST to evaluate success/failure correctly
        # The exitcode is unreliable since workers catch exceptions
        results = []
        while not result_queue.empty():
            try:
                results.append(result_queue.get_nowait())
            except Empty:
                break

        # Calculate success/failure based on actual results
        success_count = sum(1 for r in results if r.get('status') == 'success')
        # Failure count is tasks - success (handled + unhandled if any)
        # Note: 'results' might have fewer items if a worker crashed hard without putting result.
        # Fallback to tasks length for safety.
        failure_count = len(tasks) - success_count

        run_end = datetime.datetime.now()
        
        final_status = 'success' if failure_count == 0 else 'failure'
        log_batch_end(session, log_id, run_end, final_status, len(metadata), success_count, failure_count)
        
        # Send Batch Summary Email for both success and failure
        try:
            if results:
                send_batch_summary_email(batch_id, results)
        except Exception as e:
            logger.error(f"Failed to send batch summary email: {e}")

        if final_status == 'success':
            logger.info(f"Batch {batch_id} ended successfully")
        else:
            logger.error(f"Batch {batch_id} ended with {failure_count} failures")

        return 0 if final_status == 'success' else 1

    except Exception as e:
        if log_id is not None:
            log_batch_end(session, log_id, datetime.datetime.now(), 'failure', len(metadata), 0, len(metadata))
        logger.error(f"Error during batch processing: {e}")
        return 1
        # raise # Do not raise, return 1 for clean exit handling in main

    finally:
        session.close()

def worker_run(args):
    worker_id, metadata_entry, log_id, run_log_dir, result_queue = args
    logger = setup_logging_for_worker(worker_id, log_dir=run_log_dir)
    logger.info(f"Worker {worker_id} starting task for metadata entry {metadata_entry['id']}")

    Session = sessionmaker(bind=engine)
    session = Session()

    status = 'failure'
    try:
        result = process_metadata_entry(metadata_entry, log_id, logger)
        status = 'success'
        logger.info(f"Worker {worker_id} successfully processed metadata entry {metadata_entry['id']}")
        
        # Put result in queue
        if result_queue:
            result_queue.put(result)
    except Exception as e:
        logger.error(f"Error during metadata processing for worker {worker_id}: {e}")
        # Put failure result in queue
        if result_queue:
            failure_result = {
                'job_name': metadata_entry.get("job_name") or metadata_entry.get("target_table"),
                'status': 'failure',
                'error_message': str(e),
                'records_read': 0,
                'records_written': 0
            }
            result_queue.put(failure_result)
    finally:
        session.close()

    return status

def setup_logging_for_worker(worker_id, log_dir):
    today = datetime.date.today().strftime('%Y-%m-%d')
    log_dir = os.path.join(log_dir, today)
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f'worker_{worker_id}.log')
    logger = logging.getLogger(f'worker_{worker_id}')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
    
    logger.propagate = False
    return logger

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    main()
