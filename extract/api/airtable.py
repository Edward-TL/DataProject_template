"""
Basic ETL for Airtable tables
"""
from dataclasses import dataclass

from pyairtable import Api as AirtableApi
import numpy as np
import pandas as pd

from utils.logger import logger
from config_env import get_env

general_envs = get_env(context_dict = True)
config = general_envs['AIRTABLE']
AIRTABLE_PATHS = tuple([path for path in general_envs['PATHS'] if 'airtable' in path.lower()])


def remove_utc(df: pd.DataFrame, col: 'str') -> pd.Series:
    """
    Remueve el UTC de la columna para que se pueda guardar en excel
    """
    return df[col].dt.tz_localize(None)


@dataclass
class Table:
    """
    id: ID from Airtable
    folder: name of folder to save excel file
    name: name of the Table object
    general_file: prefix of the file that is going to save the table just as it is
    date_col: name of the date/datetime col that stores the record's creation time
    drop: columns to drop
    to_rename: columns you want to rename (key) and the new name (value)
    utc_date: columns with UTC format that needs to be standarized (even for excel or parquet files)
    df: starts as None so the function `get_airtable_df` can be run in other moment if you want
    paths:in case you want to store specific values in other dirs
    """
    id: str
    folder: str
    name: str
    general_file: str = "prefix_of_general_file"
    date_col: str = "COL_NAME"
    drop: tuple | None = None
    to_rename: tuple[tuple] | None = None
    rename_cols: dict | None = None
    utc_date: str | tuple | None = None
    df: pd.DataFrame | None = None
    paths: tuple | None = None

    def __post_init__(self):
        if self.to_rename is not None:
            print(len(self.to_rename))
            if len(self.to_rename) == 2:
                self.rename_cols = {self.to_rename[0]: self.to_rename[1]}


    def get_df(self) -> None:
        """
        Makes all the cleaning process form airtable to stored it as excel file
        and pandas df. It does not return anything because stores it at Table.df
        """
        api_key = config['AIRTABLE_API_KEY'],
        base_id = config['BASE_ID'],
        save_paths = AIRTABLE_PATHS

        logger.name = "get_airtable_df"
        api = AirtableApi(api_key)

        step_info(f'Table: {self.name} | Calling data from Airself.')

        airtable_data = api.table(base_id, self.id)

        try:
            records = airtable_data.all()

            step_info(f'Table: {self.name} | Records retrieved successfully')

            table_df = pd.DataFrame([record['fields'] for record in records])

            if self.drop is not None:

                step_info(f'Table: {self.name} | Eliminando tablas: {self.drop}')

                table_df = table_df.drop(
                    columns = self.drop, errors = 'ignore'
                    )
            if self.rename_cols is not None:

                step_info(f'Table: {self.name} | Renombrando columnas: {self.rename_cols}')

                table_df.rename(
                    columns = self.rename_cols,
                    inplace = True
                    )
            
            if isinstance(self.utc_date, str):
                step_info(f'Table: {self.name} | Solo una columna UTC_DATE, convirtiendo a tupla')
                self.utc_date = tuple([self.utc_date])

            if isinstance(self.utc_date, tuple):
                for col in self.utc_date:
                    step_info(f'Table: {self.name} | Convirtiendo columnas de fecha {col}')
                    table_df[col] = pd.to_datetime(table_df[col], errors='coerce')
                    if table_df[col].dt.tz is None:
                        table_df[col] = table_df[col].dt.tz_localize('UTC').dt.tz_convert('America/Mexico_City')
                    else:
                        table_df[col] = table_df[col].dt.tz_convert('America/Mexico_City')

            # REMOVE UTC
            for col in table_df.select_dtypes('datetime64[ns, UTC]').columns:
                table_df[col] = table_df[col].dt.tz_localize(None)

            step_info(f'Table: {self.name} | Convirtiendo formato de fecha')

            table_df[self.date_col] = pd.to_datetime(table_df[self.date_col], format='%Y-%m-%d')

            for col in table_df.columns:
                if table_df[col].isna().any():
                    logger.info("Table: %s | has NaT values. Replacing them with NaN")
                    table_df[col] = table_df[col].fillna(np.nan)


            # Save table in excel
            for table_path in save_paths:
                table_file = f"{table_path}/{self.general_file} {self.name}.xlsx"
                step_info(f"Table {self.name} | Guardando archivo en: {table_path}")

                table_df.to_excel(
                    table_file,
                    sheet_name = self.name,
                    index = False
                    )
                step_info(f"Table {self.name} | Data saved to Excel successfully: {table_file}")

            self.df = table_df
            step_info(f"Table {self.name} | DF stored in Table object. Check {self.name}.df")

        except Exception as e:
            err_msg = f"Failed to retrieve or save records: {str(e)}"
            logger.error(err_msg)

def step_info(msg: str) -> None:
    """
    Reduce a la mitad el codigo del logger aqui
    """
    logger.name = 'get_airtable_df'
    logger.info(msg)
