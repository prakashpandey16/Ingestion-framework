
from sqlalchemy import Table, create_engine, MetaData, func
from sqlalchemy.orm import sessionmaker
from decorators.decorators import log_execution_time, handle_errors
from conf.config import EngineManager, config
import connectorx as cx
import pyarrow as pa
from datetime import datetime
import logging
import pandas as pd
import hashlib
import re
from utils.hash_utils import add_row_full_hash
from utils.storage_utils import save_delta_copy
from pathlib import Path


def mask_connection_string(conn_str: str) -> str:
    """Masks credentials in connection strings for safe logging.
    
    Examples:
        postgresql://user:password@host:5432/db -> postgresql://***:***@host:5432/db
    """
    if not conn_str:
        return conn_str
    return re.sub(r'://[^:]+:[^@]+@', '://***:***@', conn_str)


logger = logging.getLogger(__name__)

# Load the configuration file
use_dlt = config["etl"].get("use_dlt", False)


# Configuration-Driven Backup Path
try:
    # Read path from config (defaults to relative if missing)
    raw_path_str = config["scd2engine"].get("local_backup_dir", "./data_backups")
    
    # Expand user directory (~) and resolve absolute path
    # This works for both Windows (C:\Users\...) and Linux (/home/...)
    local_backup_dir = Path(raw_path_str).expanduser().resolve()
    
    local_backup_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Resolved local backup directory: {local_backup_dir}")

except Exception as e:
    logger.warning(f"Could not resolve backup path from config: {e}. Fallback to ./data_backups")
    local_backup_dir = Path("./data_backups").resolve()
    local_backup_dir.mkdir(parents=True, exist_ok=True)

LOCAL_BACKUP_DIR = local_backup_dir

class BaseLoader:
    def __init__(self, target_table, use_connectorx, connection_string, target_schema=None):
        # Mask credentials for safe logging
        masked_conn = mask_connection_string(connection_string)
        logger.info(f"Constructing Base loader with target_table: {target_table}, use_connectorx: {use_connectorx}, connection: {masked_conn}, target schema: {target_schema}")
        
        self.target_table = target_table
        self.metadata = MetaData(schema=target_schema)
        
        if use_connectorx:
            if "postgresql" in connection_string:
                    self.connection_string = connection_string.replace("postgresql", "postgres")
            elif "mysql+pymysql" in connection_string:
                    self.connection_string = connection_string.replace("mysql+pymysql", "mysql")
        self.engine = EngineManager.get_engine(connection_string)
        self.target = Table(target_table, self.metadata, autoload_with=self.engine)
        
        self.Session = sessionmaker(bind=self.engine) if self.engine else None
        self.session = self.Session() if self.Session else None
        
        logger.info(f"Exiting Base loader with target {self.target if not use_connectorx else 'ConnectorX engine'}, connection: {masked_conn}")

    def calculate_hash(row):
         row_str=''.join(row.values.astype(str))
         return hashlib.md5(row_str.encode()).hexdigest()
    
    @handle_errors
    def add_common_columns(self, data, audit_id):
        
        if isinstance(data, pd.DataFrame):
            logger.debug(f"Adding Hash dataframe approach {datetime.now()}")
            
            data=add_row_full_hash(data,logger,audit_id,500)
            logger.info(f"after adding Hash dataframe approach{datetime.now()}")
            return data
        else:
            logger.info(f"Adding all 3 record by record approach{datetime.now()}")
            for record in data:
                record['row_full_hash'] = hashlib.md5(str(record).encode()).hexdigest()
                record['etl_date_time'] = datetime.now()
                record['audit_id'] = audit_id
            logger.info(f"Added all 3 record by record approach{datetime.now()}")
        return data



    def save_backup(self, data, entry, prefix):
        """
        Saves a backup of the data in Delta Lake format.
        Location is configured via config.toml [scd2engine] local_backup_dir.
        Category is determined by the target_schema in the metadata entry.
        """
        # Partition Columns for Delta
        now = datetime.now()
        df_backup = data.copy()
        df_backup['year'] = now.year
        df_backup['month'] = now.month
        df_backup['day'] = now.day
        partition_cols = ['year', 'month', 'day']

        logger.info(f"Saving Local Delta backup for {prefix}...")
        try:
            # Determine category from target_schema
            # User requirement: "always use target schema "
            category = entry.get('target_schema')
            if not category:
                category = 'other'
            
            # Local Path: C:\Users\praka\Desktop\Delta Local Backup\{category}\{table}_delta
            local_folder = LOCAL_BACKUP_DIR / category / f"{self.target_table}_delta"
            local_folder.parent.mkdir(parents=True, exist_ok=True)
            
            save_delta_copy(
                 df_backup,
                 str(local_folder),
                 mode='append',
                 partition_by=partition_cols
            )
            logger.info(f"Local Delta backup saved to {local_folder}")
        except Exception as e:
             logger.error(f"Could not save local Delta backup: {e}")

    @handle_errors
    def load_data(self, entry, use_connectorx,data, audit_id, source_keys=None, delete_table_name=None):
        if use_dlt:
            return self.load_data_with_dlt(data, audit_id)
        else:
            return self.load_data_default(entry,use_connectorx, data,audit_id, source_keys=None, delete_table_name=None)

    def load_data_default(self, entry,use_connectorx, data, audit_id, source_keys=None, delete_table_name=None):
        raise NotImplementedError("Subclasses must implement this method")

    def load_data_with_dlt(self, data, audit_id):
        raise NotImplementedError("Subclasses must implement this method")

    def close(self):
        if self.session:
            self.session.close()