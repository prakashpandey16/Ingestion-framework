"""Health check module for the ingestion service."""
import logging
import os
import psutil
import sys
from flask import Flask, jsonify

app = Flask(__name__)
logger = logging.getLogger(__name__)

def check_logs_writable():
    """Check if we can write to the logs directory."""
    log_dir = os.getenv('LOG_DIR', '/app/logs')
    try:
        test_file = os.path.join(log_dir, 'health_check.tmp')
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        return True
    except Exception as e:
        logger.error(f"Log directory not writable: {e}")
        return False

def check_keyfile_readable():
    """Check if the key file is readable."""
    keyfile = os.getenv('KEYFILEPATH', '/app/secret.key')
    try:
        with open(keyfile, 'r') as f:
            f.read()
        return True
    except Exception as e:
        logger.error(f"Key file not readable: {e}")
        return False

def check_system_resources():
    """Check system resources."""
    try:
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        return {
            'memory_available_mb': memory.available / (1024 * 1024),
            'disk_free_percent': disk.percent,
            'cpu_percent': psutil.cpu_percent(interval=1)
        }
    except Exception as e:
        logger.error(f"Error checking system resources: {e}")
        return None

@app.route('/health')
def health_check():
    """Health check endpoint."""
    # Check logs directory
    logs_ok = check_logs_writable()
    
    # Check key file
    keyfile_ok = check_keyfile_readable()
    
    # Check system resources
    resources = check_system_resources()
    
    status = logs_ok and keyfile_ok and resources is not None
    
    response = {
        'status': 'healthy' if status else 'unhealthy',
        'checks': {
            'logs_writable': logs_ok,
            'keyfile_readable': keyfile_ok,
            'system_resources': resources
        }
    }
    
    return jsonify(response), 200 if status else 503

def run_health_server():
    """Run the health check server."""
    from conf.config import execution_mode, config
    
    if execution_mode == 'BATCH':
        logger.warning("Health check server disabled in BATCH mode.")
        return

    port = int(os.getenv('HEALTH_PORT', '8000'))
    app.run(host='0.0.0.0', port=port)

if __name__ == '__main__':
    run_health_server()
