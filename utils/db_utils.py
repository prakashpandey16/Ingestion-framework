import logging
import time
from sqlalchemy.exc import OperationalError, InterfaceError
from conf.config import config

logger = logging.getLogger(__name__)

# Read retry settings from config
_etl_config = config.get("etl", {})
DEFAULT_MAX_RETRIES = _etl_config.get("db_max_retries", 3)
DEFAULT_RETRY_DELAY = _etl_config.get("db_retry_delay_seconds", 1)

def retry_db_operation(max_retries=None, delay=None):
    """
    Decorator to retry database operations on transient errors.
    Defaults from config.toml if not specified.
    """
    if max_retries is None:
        max_retries = DEFAULT_MAX_RETRIES
    if delay is None:
        delay = DEFAULT_RETRY_DELAY
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts <= max_retries:
                try:
                    return func(*args, **kwargs)
                except (OperationalError, InterfaceError) as e:
                    attempts += 1
                    if attempts > max_retries:
                        logger.error(f"DB Operation failed permanently after {attempts} attempts. Error: {e}")
                        raise e
                    
                    logger.warning(f"Transient DB Error encountered (Attempt {attempts}/{max_retries+1}). Retrying in {delay}s... Error: {e}")
                    
                    # Try to find and dispose the engine to reset connections
                    engine = kwargs.get('engine') or kwargs.get('dbengine')
                    
                    # If not in kwargs, search in args
                    if not engine:
                        for arg in args:
                            # Check if it looks like an engine (has dispose method)
                            # avoiding things that might have dispose but aren't engines if possible, but engines definitely have it.
                            if hasattr(arg, 'dispose') and hasattr(arg, 'connect'): 
                                engine = arg
                                break
                    
                    if engine:
                        try:
                            logger.debug("Disposing engine connection pool to force new connection on retry.")
                            engine.dispose()
                        except Exception as dispose_exc:
                            logger.warning(f"Failed to dispose engine: {dispose_exc}")
                    
                    time.sleep(delay)
            return None # Should not reach here
        return wrapper
    return decorator
