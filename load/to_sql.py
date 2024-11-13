"""
Loading sql tasks made easier
"""


from pathlib import Path
import pandas as pd
from utils.logger import logger

from connections.sql import psyco_connection, UpsertConstraint
import psycopg2.extras

def count_query(table_name: str, schema: str) -> str:
    if schema != "":
        return f'''SELECT count(*) FROM "{schema}.{table_name}"'''
    return f"SELECT count(*) FROM {table_name}"

def bring_all_query(table_name: str, schema: str) -> str:
    if schema != "":
        return f'''SELECT * FROM "{schema}.{table_name}"'''
    return f'''SELECT * FROM {table_name}'''

def create_insert_query(table, table_name, upsert) -> str:
    
    table_columns = ', '.join(table.columns)
    val_statement = ', '.join(['%s'] * len(table.columns))
    
    insert_statement = f"INSERT INTO {table_name} ({table_columns}) VALUES ({val_statement})"
    
    if upsert is None:
        execute_query = insert_statement
    else:
        execute_query = f"{insert_statement} {upsert.constraint}"

    return execute_query

def insert_many_rows(table: pd.DataFrame, schema: str = "") -> None:
    """
    Made to compare time executing many inserts
    """
    logger.name = 'insert_many_rows'
    conn = psyco_connection()
    cursor = conn.cursor()
    psycopg2.extras.register_uuid()

    # After checking table order, the name could be erased.
    # So this is left to prevent
    table_name = table.name

    cursor.execute(bring_all_query(table_name, schema))
    old_total_rows = len(cursor.fetchall())

    table_cols = [desc[0] for desc in cursor.description]
    table = table[table_cols]
    
    values = [tuple(row) for row in table.values.tolist()]
    
    table_columns = ', '.join(table.columns)
    val_statement = ', '.join(['%s'] * len(table.columns))
    insert_statement = f'''INSERT INTO "{schema}.{table_name}" ({table_columns}) VALUES ({val_statement})'''

    cursor.executemany(insert_statement, values)

    cursor.execute(count_query(table_name, schema))
    new_total_rows = cursor.fetchall()[0][0]

    # To logger
    total_new_rows = new_total_rows - old_total_rows
    logger_msg = f"Total rows inserted on {table_name.upper()}: {total_new_rows}"
    logger.info(logger_msg)

    conn.commit()
    conn.close()

def insert_many_from(
    table: pd.DataFrame | Path,
    table_name: str | None = None, schema: str = "", upsert: None | UpsertConstraint = None) -> None:
    """
    Insert by batch, preventing fails with postgres pagination limit. From
    site:

    'Psycopg will join the statements into fewer multi-statement commands,
    each one containing at most page_size statements, resulting in a reduced
    number of server roundtrips.'

    https://www.psycopg.org/docs/extras.html#fast-execution-helpers

    """
    conn = psyco_connection()
    cursor = conn.cursor()
    psycopg2.extras.register_uuid()
    logger.name = "insert_many_from"
    
    if isinstance(table, pd.DataFrame):
        if hasattr(table, 'name') and table_name is None:
            table_name = table.name
        else:
            raise TypeError("Table must have a name, but `table_name` is None")
        
    if isinstance(table, (str, Path)):
        if isinstance(table, Path) and (table_name is None):
            table_name = table.name.replace('.csv', '')

        if isinstance(table, str) and (table_name is None):
            table_name = table.split('/')[-1].replace(".csv","")
        table = pd.read_csv(table)
    
    # Counter
    cursor.execute(count_query(table.name, schema))
    old_total_rows = cursor.fetchall()[0][0]

    # Real Process
    table_cols = [desc[0] for desc in cursor.description]
    table = table[table_cols]
    execute_query = create_insert_query(table, table_name, upsert)
    values = [tuple(row) for row in table.values.tolist()]
    psycopg2.extras.execute_batch(cursor, execute_query, values)

    # Counter
    cursor.execute(count_query(table.name, schema))
    new_total_rows = cursor.fetchall()[0][0]

    # To logger
    total_new_rows = new_total_rows - old_total_rows
    logger_msg = f"Total rows inserted on {table_name.upper()}: {total_new_rows}"
    logger.info(logger_msg)

    conn.commit()
    conn.close()