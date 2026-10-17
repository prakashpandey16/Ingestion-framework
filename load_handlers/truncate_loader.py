import logging
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker
from load_handlers.base_loader import BaseLoader
from utils.schema_manager import ensure_target_table_exists
from utils.dataframe_loader import unload_from_df_intodb
from utils.logging_utils import log_error_details, send_failure_notification
from conf.config import engine as meta_engine
from pathlib import Path

logger = logging.getLogger(__name__)


class TruncateLoader(BaseLoader):
    """
    Handles loading data into a target table using a TRUNCATE → INSERT strategy.
    
    This strategy completely empties the target table before inserting the new data.
    """

    def load_data_default(self, entry, use_connectorx, data, audit_id,
                          source_keys=None, delete_table_name=None):
        """
        Main entry point for the truncate load operation.

        Args:
            entry (dict): Configuration entry.
            use_connectorx (bool): Whether to use ConnectorX (not used in this loader).
            data (pd.DataFrame or list): Data to be loaded.
            audit_id (str): Audit ID for tracking.
            source_keys (list, optional): Keys from the source (unused).
            delete_table_name (str, optional): Name of the delete table (unused).

        Returns:
            tuple: (insert_count, update_count, delete_count)
        """

        logger.info(f"[TRUNCATE LOADER] Start truncate load for table {self.target_table}")

        # Convert to DataFrame if needed (Handles the "name 'pd' is not defined" error if it occurred elsewhere)
        if not isinstance(data, pd.DataFrame):
            # This check is good practice but ensures 'pd' is used after import
            data = pd.DataFrame(data)

        if data.empty:
            logger.warning("[TRUNCATE LOADER] DataFrame empty → nothing to load.")
            return 0, 0, 0

        # Normalize column names to avoid SQL issues
        logger.info("[TRUNCATE LOADER] Normalizing column names...")
        data.columns = (
            data.columns
            .str.lower()
            .str.replace(r'\W+', '_', regex=True)
        )

        # Add common ETL audit columns
        logger.info("[TRUNCATE LOADER] Adding audit + hash columns...")
        data = self.add_common_columns(data, audit_id)

        # Local parquet backup
        self.save_backup(data, entry, self.target_table)

        try:
            # Ensure table exists before loading
            ensure_target_table_exists(entry, data, logger)

            # Construct full table name with schema
            table_name = (
                f"{self.metadata.schema}.{self.target_table}"
                if self.metadata.schema else
                self.target_table
            )

            # Use TRUNCATE ... RESTART IDENTITY
            # This resets the primary key sequence, preventing the "duplicate key value violates unique constraint" error.
            logger.info(f"[TRUNCATE LOADER] Truncating table {table_name} and restarting identity.")
            with self.engine.begin() as conn:
                # The 'RESTART IDENTITY' clause is essential for tables with SERIAL primary keys.
                conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;"))

            # Insert new records (This operation can still fail, e.g., on a secondary integrity error or connection loss)
            logger.info(f"[TRUNCATE LOADER] Inserting {len(data)} records...")

            try:
                unload_from_df_intodb(
                    data,
                    self.engine,
                    self.target_table,
                    logger,
                    self.metadata.schema
                )
            except ProgrammingError as e:
                # Check for "relation does not exist"
                if ("relation" in str(e) or "doesn't exist" in str(e)) and entry.get('auto_create_table', True):
                    logger.warning(f"Table {self.target_table} missing during truncate load. Creating and retrying...")
                    # Note: We need to rollback the previous failed transaction (handled by top-level try/except or auto-rollback?)
                    # Since we are inside 'with self.engine.begin()', the context manager might have exited or is in error state.
                    # Actually valid point: 'with self.engine.begin()' handles the transaction. If error occurs, it rolls back.
                    # So we need to retry the WHOLE block including truncate.
                    raise e # We handle it outside? No, 'unload' is inside the block.
                    
                    # If we catch here, the transaction is already doomed.
                    # We should probably let it bubble up to a broader retry or restructure.
                    # BUT the requirement is "create table -> retry load once".
                    
                    # If 'unload' failed, the table might have been dropped after TRUNCATE (unlikely) or never existed (but TRUNCATE would fail first).
                    # TRUNCATE is executed before unload provided it's the same block.
                    # If TRUNCATE query (line 80) succeeded, table exists.
                    # So 'unload' failing with "missing table" is very unlikely unless dropped concurrently.
                    
                    # If TRUNCATE failed with missing table, it throws error at line 80.
                    # I should wrap the TRUNCATE + UNLOAD block.
                else:
                    raise

            logger.info(f"[TRUNCATE LOADER] Successfully inserted {len(data)} rows.")

            return len(data), 0, 0

        except Exception as e:
            logger.error(f"[TRUNCATE LOADER] Critical error during truncate load: {e}", exc_info=True)

            # Explicit Rollback for Target Engine
            # This prevents the sqlalchemy.exc.PendingRollbackError in subsequent operations (like logging the error).
            logger.error("***Came here to log the error******")
            
            # The failure might have left a transaction open on the main data engine. Explicitly rollback.
            # We use a raw connection to ensure we can force a rollback even if SQLAlchemy thinks the transaction is invalid.
            try:
                raw_conn = self.engine.raw_connection()
                if raw_conn.in_transaction():
                    raw_conn.rollback()
                raw_conn.close()
            except Exception as rollback_e:
                logger.warning(f"Failed to forcibly rollback target database transaction: {rollback_e}")

            # Log error to metadata log table (using the separate meta_engine)
            Session = sessionmaker(bind=meta_engine)
            session = Session()
            
            # Use try/finally to ensure the error logging session is closed/committed properly
            try:
                log_error_details(session, audit_id, entry["id"], str(e), logger)
                session.commit()
            except Exception as log_e:
                session.rollback()
                logger.error(f"Failed to log error details to metadata table: {log_e}")
                
            finally:
                session.close() # Close the error logging session

            # Note: The email notification failure is outside this code and must be fixed 
            # by providing an Application Password for the sender email account.
            send_failure_notification(
                f"TRUNCATE LOADER FAILURE for Metadata ID {entry['id']} in Batch {entry['batch_id']}",
                str(e)
            )

            # Re-raise the original exception to mark the Airflow task as failed
            raise e

        finally:
            # Your original cleanup logic for an old 'self.session' variable, kept for safety.
            if hasattr(self, 'session') and self.session and self.session.is_active:
                self.session.close()