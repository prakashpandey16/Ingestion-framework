import functools
import logging
import time

def log_execution_time(func):
    @functools.wraps(func)
    def wrapper_log_execution_time(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(f"Executed {func.__name__} in {end_time - start_time:.4f} seconds")
        return result
    return wrapper_log_execution_time

def handle_errors(func):
    @functools.wraps(func)
    def wrapper_handle_errors(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error in {func.__name__}: {e}")
            raise
    return wrapper_handle_errors
