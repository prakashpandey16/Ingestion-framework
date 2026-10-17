import logging
from load_handlers.base_loader import BaseLoader
import pandas as pd
from load_handlers.dlt_loader import DLTAppendLoader
from utils.logging_utils import log_batch_start, log_error_details, log_audit_details,send_failure_notification
from conf.config import engine
from sqlalchemy.orm import sessionmaker
from utils.dataframe_loader import unload_from_df_intodb
from utils.schema_manager import ensure_target_table_exists
from pathlib import Path
from sqlalchemy.exc import ProgrammingError

logger = logging.getLogger(__name__)

class AppendLoader(BaseLoader):
    """
    Loader class for appending data to an existing target table.
    """
    def load_data_default(self, entry, use_connectorx, data, audit_id, source_keys=None, delete_table_name=None):
        """
        Appends data to the target table.

        Args:
            entry (dict): Configuration entry.
            use_connectorx (bool): Whether to use ConnectorX.
            data: Data to be appended (DataFrame or list of dicts).
            audit_id (str): Audit ID for tracking.
            source_keys (list, optional): Source keys (unused).
            delete_table_name (str, optional): Delete table name (unused).
        """
        if not isinstance(data, pd.DataFrame):
                data = pd.DataFrame(data)
        
        logger.info(f"Starting append load with data:")
        try:
            data = self.add_common_columns(data, audit_id)
            logger.info(f"Added common columns")

            # Local parquet backup
            self.save_backup(data, entry, self.target.name)

            try:
                unload_from_df_intodb(data,self.engine,self.target.name,logger,self.metadata.schema)
            except ProgrammingError as e:
                # Check for "relation does not exist" or similar DB errors
                if ("relation" in str(e) or "doesn't exist" in str(e)) and entry.get('auto_create_table', True):
                    logger.warning(f"Table {self.target.name} missing during append load. Creating and retrying...")
                    ensure_target_table_exists(entry, data, logger)
                    # Retry load once
                    unload_from_df_intodb(data,self.engine,self.target.name,logger,self.metadata.schema)
                else:
                    raise
            
            logger.info(f"Successfully inserted {len(data)} records.")
        except Exception as e:
            logger.error(f"Error during append load: {e}", exc_info=True)
            Session = sessionmaker(bind=engine)
            session = Session()
            
            log_error_details(session, audit_id, entry['id'], str(e), logger)
            logger.error(f"logged now")
            send_failure_notification(f"Metadata ID {entry['id']} Batch ID {entry['batch_id']} Failed", str(e))
            raise
        finally:
            self.session.close()

    def load_data_with_dlt(self, data, audit_id):
        dlt_loader = DLTAppendLoader(self.target.name, self.engine.url, self.metadata.schema)
        dlt_loader.load_data(data, audit_id)
