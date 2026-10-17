import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from conf.config import engine, metadata_table

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)
session = Session()

def fetch_metadata(batch_id):
    try:
        logger.info(f"Fetching metadata for batch_id: {batch_id}")
        results = session.execute(
            metadata_table.select()
            .where(metadata_table.c.batch_id == batch_id)
            .where(metadata_table.c.active == True)
            ).fetchall()

        if not results:
            logger.warning(f"No metadata found for batch_id: {batch_id}")
        else:
            logger.info(f"Fetched {len(results)} metadata entries for batch_id: {batch_id}")


        # Get column names from the table definition
        columns = metadata_table.columns.keys()

        # Convert ResultProxy tuples to dictionaries
        metadata_dicts = []
        for result in results:
            metadata_dict = {column: value for column, value in zip(columns, result)}

            # Add default values for missing optional columns
            optional_columns = ['use_hash', 'delete_handling', 'delete_table_name']
            for column in optional_columns:
                if column not in metadata_dict:
                    metadata_dict[column] = None  # Default value for missing columns

            metadata_dicts.append(metadata_dict)

        return metadata_dicts

        

    except Exception as e:
        logger.error(f"Error fetching metadata for batch_id {batch_id}: {e}", exc_info=True)
        raise

    finally:
        session.close()
