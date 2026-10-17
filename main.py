import logging
import multiprocessing
import sched
import time
import os
import datetime
from sqlalchemy.orm import sessionmaker
from conf.config import config, engine
from Batch.log_manager import setup_logging,setup_logging_for_batch
from Batch.batch_scheduler import schedule_batches
from utils.logging_utils import log_batch_start, log_batch_end
from conf.metadata_loader import fetch_metadata
from etl_runner import process_metadata_entry
from utils.mail_notifier import send_batch_summary_email
from queue import Empty, Queue

task_queue = Queue()

def main():
    base_worker_id = 0  # Hardcoded base worker ID
    run_log_dir = config["etl"]["log_dir"]
    print(f"Starting ETL process as base worker with worker {base_worker_id} logs at {run_log_dir}")
    setup_logging(base_worker_id, run_log_dir)
    logger = logging.getLogger('main')
    logger.info("Starting ETL process as base worker")
    
    scheduler = sched.scheduler(time.time, time.sleep)
    interval = config["etl"].get("interval_minutes", 5) * 60  # Default to 5 minutes if not set

    def run_batches():
        nonlocal interval
        logger.info("Running scheduled batch check")
        
        if task_queue.empty():
            eligible_batches = schedule_batches()
            
            if eligible_batches:
                num_workers = config["etl"]["max_workers"]
                logger.info(f"Found eligible batches and num of workers: {num_workers}")
                for batch in eligible_batches:
                    logger.info(f"Adding batch {batch.batch_id} to queue")
                    task_queue.put(batch)

                ctx = multiprocessing.get_context("spawn")
                with ctx.Pool(num_workers) as pool:
                    while not task_queue.empty():
                        try:
                            logger.info("Entering while loop to process task queue")
                            batch = task_queue.get_nowait()
                            logger.info(f"Processing batch {batch.batch_id}")
                            pool.apply_async(run_batch_wrapper, args=(batch, run_log_dir))
                        except Empty:
                            logger.info("No more batches to process")
                            break

                    pool.close()
                    pool.join()
            else:
                logger.info("No eligible batches found")
        else:
            logger.info("Task queue is not empty, skipping batch scheduling")

        scheduler.enter(interval, 1, run_batches)
    
    scheduler.enter(0, 1, run_batches)
    scheduler.run()

def run_batch_wrapper(batch, run_log_dir):
    logger = logging.getLogger('main')
    logger.info(f"Starting run_batch_wrapper for batch {batch.batch_id}")
    run_batch(batch, run_log_dir)
    logger.info(f"Finished run_batch_wrapper for batch {batch.batch_id}")

def run_batch(batch, run_log_dir):
    Session = sessionmaker(bind=engine)
    session = Session()
    logger = setup_logging_for_batch(batch.batch_id, run_log_dir)
    process_id = os.getpid()
    logger.info(f"Starting run_batch with PID {process_id} and batch {batch.batch_id}")

    run_start = datetime.datetime.now()
    log_id = None
    try:
        log_id = log_batch_start(session, batch.batch_id, run_start)
        logger.info(f"Batch {batch.batch_id} started with log_id {log_id}")
        
        metadata = fetch_metadata(batch.batch_id)
        tasks = [(worker_id, entry, log_id, run_log_dir) for worker_id, entry in enumerate(metadata)]
        
        with multiprocessing.get_context("spawn").Pool(len(tasks)) as pool:
            results = pool.map(worker_run, tasks)

        logger.info(f"Batch {batch.batch_id} processing completed")
        if results:
            success_count = sum(1 for r in results if r and r.get('status') == 'success')
            failure_count = sum(1 for r in results if r and r.get('status') == 'failure')
        else:
            success_count = 0
            failure_count = 0

        run_end = datetime.datetime.now()
        log_batch_end(session, log_id, run_end, 'success', len(metadata), success_count, failure_count)
        
        # Send consolidated email
        send_batch_summary_email(batch.batch_id, results)
        
        logger.info(f"Batch {batch.batch_id} ended successfully")

    except Exception as e:
        if log_id is not None:
            log_batch_end(session, log_id, datetime.datetime.now(), 'failure', len(metadata), 0, len(metadata))
        logger.error(f"Error during batch processing: {e}")
        raise

    finally:
        session.close()

def worker_run(args):
    worker_id, metadata_entry, log_id, run_log_dir = args
    logger = setup_logging_for_worker(worker_id, log_dir=run_log_dir)
    logger.info(f"Worker {worker_id} starting task for metadata entry {metadata_entry['id']}")

    Session = sessionmaker(bind=engine)
    session = Session()

    result = None
    try:
        result = process_metadata_entry(metadata_entry, log_id, logger)
        logger.info(f"Worker {worker_id} successfully processed metadata entry {metadata_entry['id']}")
    except Exception as e:
        logger.error(f"Error during metadata processing for worker {worker_id}: {e}")
        # Construct failure result for reporting
        result = {
            'status': 'failure', 
            'error_message': str(e),
            'job_name': metadata_entry.get('job_name', f"job_{metadata_entry['id']}"),
            'metadata_id': metadata_entry['id'],
            'records_read': 0, 
            'records_written': 0
        }
    finally:
        session.close()

    return result

# def setup_logging_for_batch(batch_id, log_dir):
#     log_file = os.path.join(log_dir, f'batch_{batch_id}.log')
#     logger = logging.getLogger(f'batch_{batch_id}')
#     logger.setLevel(logging.INFO)
#     formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    
#     file_handler = logging.FileHandler(log_file)
#     file_handler.setFormatter(formatter)
#     file_handler.setLevel(logging.INFO)
    
#     stream_handler = logging.StreamHandler()
#     stream_handler.setFormatter(formatter)
#     stream_handler.setLevel(logging.INFO)
    
#     if not logger.handlers:
#         logger.addHandler(file_handler)
#         logger.addHandler(stream_handler)
    
#     logger.propagate = False
#     return logger

def setup_logging_for_worker(worker_id, log_dir):
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

def run_batch_with_id(batch_id):
    """Function to run main_batch_run.py with a specific batch ID"""
    logger = logging.getLogger('scheduler')
    logger.info(f"Starting batch with ID: {batch_id}")
    try:
        # Import the subprocess module
        import subprocess
        # Run the main_batch_run.py with the specified batch ID
        subprocess.run([sys.executable, 'main_batch_run.py', str(batch_id)], check=True)
        logger.info(f"Successfully completed batch {batch_id}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running batch {batch_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in batch {batch_id}: {e}")

def schedule_batch_run(batch_id, interval_minutes=60):
    """Schedule a batch to run at regular intervals using sched"""
    import time
    import threading
    
    # Set up logging for the scheduler
    logger = logging.getLogger('scheduler')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('scheduler.log'),
            logging.StreamHandler()
        ]
    )
    
    scheduler = sched.scheduler(time.time, time.sleep)
    interval_seconds = interval_minutes * 60
    
    def run_periodically():
        """Function to run the batch and schedule the next run"""
        run_batch_with_id(batch_id)
        # Schedule the next run
        scheduler.enter(interval_seconds, 1, run_periodically, ())
    
    # Start the scheduler in a separate thread
    def run_scheduler():
        logger.info(f"Starting scheduler for batch {batch_id} with {interval_minutes} minute interval")
        # Schedule the first run immediately
        scheduler.enter(0, 1, run_periodically, ())
        scheduler.run()
    
    # Start the scheduler in a daemon thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler...")
        # This will stop the scheduler thread as it's a daemon thread

if __name__ == "__main__":
    import sys
    
    # Check if we should start the scheduler with a specific batch ID
    if len(sys.argv) > 1 and sys.argv[1] == '--schedule':
        batch_id = sys.argv[2] if len(sys.argv) > 2 else '11'  # Default to batch ID 11
        interval = int(sys.argv[3]) if len(sys.argv) > 3 else 60  # Default to 60 minutes
        schedule_batch_run(batch_id, interval)
    else:
        # Run the normal main function
        # config = toml.load("C:/Users/ashish.k/OneDrive - CONNECT INDIA E-COMMERCE SERVICES PVT LTD/Desktop/ingestion-framework/config.toml")
        main()
