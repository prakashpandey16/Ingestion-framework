"""
Utility functions for managing batch configurations in the database.
"""
from sqlalchemy import create_engine, MetaData, Table, select, and_
from sqlalchemy.orm import sessionmaker
from datetime import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_utils.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('batch_utils')

def get_db_connection():
    """Create and return a database session."""
    from conf.config import engine
    Session = sessionmaker(bind=engine)
    return Session()

def add_batch_config(batch_id, frequency, start_time, batch_name, description=None, active=True):
    """
    Add a new batch configuration to the batch_config_table.
    
    Args:
        batch_id (int): The ID of the batch
        frequency (str): Frequency of the batch ('high', 'medium', 'low')
        start_time (str): Start time in 'HH:MM:SS' format
        batch_name (str): Name of the batch
        description (str, optional): Description of the batch
        active (bool, optional): Whether the batch is active. Defaults to True.
        
    Returns:
        bool: True if the batch was added successfully, False otherwise
    """
    session = None
    try:
        from conf.config import batch_config_table
        
        # Parse the time string to time object
        try:
            time_obj = time(*map(int, start_time.split(':')))
        except (ValueError, AttributeError):
            logger.error(f"Invalid time format: {start_time}. Expected format: 'HH:MM:SS'")
            return False
            
        # Validate frequency
        if frequency.lower() not in ['high', 'medium', 'low']:
            logger.error(f"Invalid frequency: {frequency}. Must be one of: 'high', 'medium', 'low'")
            return False
            
        session = get_db_connection()
        
        # Check if batch_id already exists
        existing = session.execute(
            select(batch_config_table).where(batch_config_table.c.batch_id == batch_id)
        ).scalar()
        
        if existing:
            logger.warning(f"Batch with ID {batch_id} already exists. Updating instead.")
            # Update existing record
            stmt = (
                batch_config_table.update()
                .where(batch_config_table.c.batch_id == batch_id)
                .values(
                    frequency=frequency.lower(),
                    start_time=time_obj,
                    active=active,
                    batch_name=batch_name,
                    batch_description=description
                )
            )
        else:
            # Insert new record
            stmt = batch_config_table.insert().values(
                batch_id=batch_id,
                frequency=frequency.lower(),
                start_time=time_obj,
                active=active,
                batch_name=batch_name,
                batch_description=description
            )
        
        session.execute(stmt)
        session.commit()
        logger.info(f"Successfully {'updated' if existing else 'added'} batch configuration: {batch_name} (ID: {batch_id})")
        return True
        
    except Exception as e:
        logger.error(f"Error adding/updating batch configuration: {e}", exc_info=True)
        if session:
            session.rollback()
        return False
    finally:
        if session:
            session.close()

def list_batch_configs():
    """
    List all batch configurations in the database.
    
    Returns:
        list: List of batch configuration dictionaries
    """
    session = None
    try:
        from conf.config import batch_config_table
        
        session = get_db_connection()
        result = session.execute(select(batch_config_table)).fetchall()
        
        # Convert to list of dictionaries
        batches = []
        for row in result:
            batch = {
                'batch_id': row.batch_id,
                'frequency': row.frequency,
                'start_time': str(row.start_time),
                'active': row.active,
                'batch_name': row.batch_name,
                'batch_description': row.batch_description
            }
            batches.append(batch)
            
        return batches
        
    except Exception as e:
        logger.error(f"Error listing batch configurations: {e}", exc_info=True)
        return []
    finally:
        if session:
            session.close()

if __name__ == "__main__":
    # Example usage
    # Add a new batch configuration
    add_batch_config(
        batch_id=12,
        frequency='high',
        start_time='16:41:00',
        batch_name='s3_to_postgres',
        description='Extracting csv files from s3 to postgresql tables',
        active=True
    )
    
    # List all batch configurations
    print("\nCurrent batch configurations:")
    for batch in list_batch_configs():
        print(f"ID: {batch['batch_id']}, Name: {batch['batch_name']}, "
              f"Frequency: {batch['frequency']}, Time: {batch['start_time']}, "
              f"Active: {batch['active']}")
        if batch['batch_description']:
            print(f"  Description: {batch['batch_description']}")
        print("-" * 50)
