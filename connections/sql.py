"""
Creates connections to SQL Databases
"""
import time

from dataclasses import dataclass

from typing import Literal
from config_env import get_env
from sqlalchemy import create_engine
import psycopg2
import pandas as pd

@dataclass
class UpsertConstraint:
    """
    ref_col: The column(s) needed at "ON CONFLICT (col)" query part
    update_cols: All the columns that will be updated on conclict scenario
    """
    ref_col: str | tuple
    update_cols: tuple

    def __post_init__(self):
        if isinstance(self.ref_col, str):
            self.ref_col = tuple([self.ref_col])

        update_cols_ref = ",".join(
            [
                f"{col} = EXCLUDED.{col}" for col in self.update_cols
            ]
        )
        self.constraint = f"ON CONFLICT {self.ref_col} DO UPDATE SET {update_cols_ref};"

general_envs = get_env(context_dict = True)
DB = general_envs['SQL']

def make_sql_url(sql_engine:Literal['mysql', 'postgres'] = 'mysql'):
    """
    Creates de sql url used for the engine to connect
    """
    engines = {
        'mysql': 'mysql+pymysql',
        'postgres': 'postgresql'
    }

    sql_url = f"{engines[sql_engine]}://{DB['USER']}:{DB['PASSWORD']}@{DB['HOST']}/{DB['DB']}?charset=utf8mb4"

    return sql_url
    
DB_URL = make_sql_url(DB['ENGINE'])
DB_ENGINE = create_engine(DB_URL)

def connect_to_postgres(env_vars: dict = DB):
    conn = psycopg2.connect(
            database = env_vars["DB"],
            user = env_vars["USER"],
            password = env_vars["PASSWORD"],
            host = env_vars["HOST"],
            port = env_vars["PORT"]   
        )
    
    conn.autocommit = True

    return conn.cursor()



def query(sql: str) -> pd.DataFrame:
    if DB['ENGINE'] == "mysql":
        cnx = DB_ENGINE.connect()
    else:
        cnx = connect_to_postgres()

    cursor = cnx.execute(sql)
    # The execute returns a list of tuples:
    tuples_list = cursor.fetchall()

    # Now we need to transform the list into a pandas DataFrame:
    df = pd.DataFrame(tuples_list)
    
    # add the columns
    if DB['ENGINE'] == 'postgres':
        df.columns = [i[0] for i in cursor.description]
    # mysql
    else:
        df.columns = [i[0] for i in cursor.cursor.description]
    cursor.close()
    
    return df