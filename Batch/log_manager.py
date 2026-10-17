import logging
import os
import datetime

def setup_logging(base_worker_id, log_dir):
    today = datetime.date.today().strftime('%Y-%m-%d')
    log_dir_expanded = os.path.join(log_dir, today)
    print(f"[DEBUG] setup_logging_for_batch: log_dir argument: {log_dir}")
    print(f"[DEBUG] setup_logging_for_batch: expanded log_dir: {log_dir_expanded}")
    os.makedirs(log_dir_expanded, exist_ok=True)
    
    log_file = os.path.join(log_dir, f'worker_{base_worker_id}.log')
    logger = logging.getLogger(f'worker_{base_worker_id}')
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
    logger.info(f"Logging setup complete for worker {base_worker_id}")

def setup_logging_for_batch(batch_id, log_dir):
    today = datetime.date.today().strftime('%Y-%m-%d')
    log_dir_expanded = os.path.join(log_dir, today)
    print(f"[DEBUG] setup_logging_for_batch: log_dir argument: {log_dir}")
    print(f"[DEBUG] setup_logging_for_batch: expanded log_dir: {log_dir_expanded}")
    os.makedirs(log_dir_expanded, exist_ok=True)

    log_file = os.path.join(log_dir_expanded, f'batch_{batch_id}.log')
    logger = logging.getLogger(f'batch_{batch_id}')
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

def setup_logging_for_worker(worker_id, log_dir):
    today = datetime.date.today().strftime('%Y-%m-%d')
    log_dir_expanded = os.path.join(log_dir, today)
    print(f"[DEBUG] setup_logging_for_worker: log_dir argument: {log_dir}")
    print(f"[DEBUG] setup_logging_for_worker: expanded log_dir: {log_dir_expanded}")
    os.makedirs(log_dir_expanded, exist_ok=True)

    log_file = os.path.join(log_dir_expanded, f'worker_{worker_id}.log')
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
