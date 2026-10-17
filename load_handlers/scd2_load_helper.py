import pandas as pd
import duckdb
import hashlib
from datetime import date, datetime
from sqlalchemy import Table, create_engine, MetaData, select, update, and_, text
from sqlalchemy.orm import sessionmaker
from load_handlers.base_loader import BaseLoader
from load_handlers.dlt_loader import DLTSCD2Loader
import logging
import toml

logger = logging.getLogger(__name__)




# load_data_with_db removed (logic resides in scd2_loader.py)

def load_data_with_duckdb(self, data, audit_id, source_keys=None, delete_table_name=None):
    """
    Loads data using DuckDB as an intermediate processing engine.

    Args:
        self (SCD2Loader): The loader instance.
        data: Data to load.
        audit_id (str): Audit identifier.
        source_keys (list, optional): Source keys.
        delete_table_name (str, optional): Name of table handling deletes.

    Returns:
        tuple: (insert_count, update_count)
    """
    logger.info(f"Starting load_data with DuckDB: source_keys: {source_keys}, delete_table_name: {delete_table_name}")

    duckdb_conn = duckdb.connect(database=':memory:')
    temp_table_name = "temp_table"

    # Load target table data from SQLAlchemy into DuckDB
    target_df = pd.read_sql_table(self.target_table, self.engine, schema=self.metadata.schema)
    logger.info(f"reading the target table as a dataframe {self.target_table}")
    duckdb_conn.register('target_table', target_df)

    # Create temp table in DuckDB and insert data
    duckdb_conn.execute(f"CREATE TABLE {temp_table_name} AS SELECT * FROM target_table WHERE 1=0")
    duckdb_conn.execute(f"INSERT INTO {temp_table_name} VALUES {','.join(str(tuple(d.values())) for d in data)}")

    logger.info(f"Created temp table in duck db {temp_table_name}")
    
    business_key_conditions = " AND ".join(
        [f"t.{col} = tmp.{col}" for col in self.business_key_columns]
    )
    hash_condition = " OR ".join(
        [f"t.{col} != tmp.{col}" for col in target_df.columns if col not in self.business_key_columns + ['effective_date', 'end_date', 'is_current', 'etl_date_time', 'audit_id']]
    )

    # Insert new records
    insert_count = duckdb_conn.execute(f"""
        INSERT INTO target_table
        SELECT tmp.*, CURRENT_DATE, DATE '9999-12-31', True, CURRENT_TIMESTAMP, {audit_id}
        FROM {temp_table_name} tmp
        LEFT JOIN target_table t
        ON {business_key_conditions}
        WHERE t.{self.business_key_columns[0]} IS NULL
    """).rowcount
    logger.info(f"Insert completed {insert_count}")

    # Update existing records
    update_count = duckdb_conn.execute(f"""
        UPDATE target_table t
        SET is_current = False, end_date = CURRENT_DATE
        FROM {temp_table_name} tmp
        WHERE {business_key_conditions} AND ({hash_condition})
    """).rowcount
    logger.info(f"Update completed {update_count}")

    duckdb_conn.execute(f"""
        INSERT INTO target_table
        SELECT tmp.*, CURRENT_DATE, DATE '9999-12-31', True, CURRENT_TIMESTAMP, {audit_id}
        FROM {temp_table_name} tmp
        INNER JOIN target_table t
        ON {business_key_conditions}
        WHERE ({hash_condition})
    """)

    # Retrieve final target table as a dataframe
    final_target_df = duckdb_conn.execute("SELECT * FROM target_table").fetchdf()
    
    # Write final target dataframe to the target database as a temporary table
    final_target_df.to_sql(f"{self.target_table}_temp", self.engine, schema=self.metadata.schema, if_exists='replace', index=False)
    
    # Rename tables in the target database
    with self.engine.connect() as conn:
        conn.execute(f"DROP TABLE IF EXISTS {self.metadata.schema}.{self.target_table}_backup")
        conn.execute(f"ALTER TABLE {self.metadata.schema}.{self.target_table} RENAME TO {self.target_table}_backup")
        conn.execute(f"ALTER TABLE {self.metadata.schema}.{self.target_table}_temp RENAME TO {self.target_table}")
        
    
    logger.info(f"Renamed tables in target database. Final table is {self.metadata.schema}.{self.target.name}.")
    return insert_count, update_count

def load_data_with_pandas(self, data, audit_id, source_keys=None, delete_table_name=None):
    """
    Loads data using Pandas for merging and filtering logic.

    Args:
        self (SCD2Loader): The loader instance.
        data: Data to load.
        audit_id (str): Audit identifier.
        source_keys (list, optional): Source keys.
        delete_table_name (str, optional): Name of table handling deletes.

    Returns:
        tuple: (insert_count, update_count)
    """
    logger.info(f"Starting load_data with Pandas: source_keys: {source_keys}, delete_table_name: {delete_table_name}")

    target_df = pd.read_sql_table(self.target.name, self.engine, schema=self.metadata.schema)
    temp_df = pd.DataFrame(data)

    merged_df = temp_df.merge(target_df, on=self.business_key_columns, suffixes=('_new', '_old'), how='left')

    # Insert new records
    new_records = merged_df[merged_df[f'{self.business_key_columns[0]}_old'].isnull()]
    insert_count = len(new_records)
    if not new_records.empty:
        new_records = new_records.drop([col for col in new_records.columns if col.endswith('_old')], axis=1)
        new_records['effective_date'] = date.today()
        new_records['end_date'] = date(9999, 12, 31)
        new_records['is_current'] = True
        new_records['etl_date_time'] = datetime.now()
        new_records['audit_id'] = audit_id
        new_records.to_sql(self.target.name, self.engine, schema=self.metadata.schema, if_exists='append', index=False)

    # Update existing records
    changed_records = merged_df[merged_df.apply(lambda row: any(row[col] != row[f'{col}_old'] for col in temp_df.columns if col not in self.business_key_columns), axis=1)]
    update_count = len(changed_records)
    if not changed_records.empty:
        for _, row in changed_records.iterrows():
            business_key_conditions = [self.target.c[col] == row[col] for col in self.business_key_columns]
            self.session.execute(
                update(self.target)
                .where(and_(*business_key_conditions))
                .where(self.target.c.is_current == True)
                .values(is_current=False, end_date=date.today())
            )
            record = {col: row[col] for col in temp_df.columns}
            record.update({
                'effective_date': date.today(),
                'end_date': date(9999, 12, 31),
                'is_current': True,
                'etl_date_time': datetime.now(),
                'audit_id': audit_id
            })
            self.session.execute(self.target.insert().values(record))

    logger.info("Load data completed using Pandas.")
    return insert_count, update_count