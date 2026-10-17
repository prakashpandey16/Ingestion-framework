
import logging
from conf.metadata_loader import fetch_metadata
from Extract.extract_processor import extract_data
from Transform.transform_executor import transform_data
from utils.db_config_loader import get_db_connection_string
from load_handlers.scd2_loader import SCD2Loader
from load_handlers.append_loader import AppendLoader
from load_handlers.truncate_loader import TruncateLoader
from decorators.decorators import log_execution_time, handle_errors
from utils.schema_manager import ensure_target_table_exists
from utils.logging_utils import log_batch_start, log_batch_end, log_audit_details
from sqlalchemy.orm import sessionmaker
import datetime
import pandas as pd
from conf.config import engine
from dq.dq_processor import run_dq_in_motion, run_dq_at_rest



@handle_errors
def load_data(entry, data, log_id,logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)
    
    
    target_db_id = entry['target_db_id']

    target_table = entry['target_table']
 
    load_type = entry['load_type']
    business_key = entry['business_key'].split(',') if entry['business_key'] else []
    use_hash = entry.get('use_hash', False)
    delete_handling = entry['delete_handling']
    delete_table_name = entry.get('delete_table_name', None)
 
    target_schema = entry['target_schema']
    
    dq_in_motion = entry['dq_in_motion']
    dq_at_rest = entry['dq_at_rest']

    is_dataframe = isinstance(data, pd.DataFrame)

    use_connectorx = entry['use_connectorx']


    if dq_in_motion:
        run_dq_in_motion(data, target_table, logger)
        


    # Get connection strings
    
    target_connection_string,target_db_type = get_db_connection_string(target_db_id, is_source=False)
    
    
    source_keys = None
    if delete_handling == 'full_scan':
        if is_dataframe:
            source_keys = data[business_key[0]].tolist()
        else:
            source_keys = [record[business_key[0]] for record in data]
    
    
    
    if load_type == 'scd2':
        loader = SCD2Loader(target_table,use_connectorx, target_connection_string, target_schema,business_key,use_hash, delete_handling)
        
        insert_count, update_count, delete_count = loader.load_data(entry,use_connectorx,data, log_id,source_keys, delete_table_name)
        

        
    
    elif load_type == 'append':
        loader = AppendLoader(target_table,use_connectorx, target_connection_string,target_schema)
        loader.load_data(entry,use_connectorx,data,log_id)
        loader.close()
        logger.info(f"AppendLoader - records inserted: {len(data)}")
        insert_count = len(data)
        update_count = 0
        delete_count = 0
    
    elif load_type == 'truncate':
        loader = TruncateLoader(target_table, use_connectorx,target_connection_string,target_schema)
        loader.load_data(entry,use_connectorx,data,log_id)
        loader.close()
        logger.info(f"TruncateLoader - records inserted: {len(data)}")
        insert_count = len(data)
        update_count = 0
        delete_count = 0
    
    else:
        logger.error(f"Unsupported load type: {load_type}")
        raise ValueError(f"Unsupported load type: {load_type}")
    
    if dq_at_rest:
        run_dq_at_rest(target_table, logger)

    return insert_count, update_count, delete_count