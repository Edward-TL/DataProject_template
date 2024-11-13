import os
import sys

from pathlib import Path
from dataclasses import dataclass

import pandas as pd
from pyspark.sql import SparkSession
from pyspark import pandas as ps

from extract.files import get_files_from_root
from helpers import FILE_TYPES

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark_session = SparkSession.builder \
    .master('local') \
    .appName('Nubank-Case') \
    .getOrCreate()

@dataclass
class SparkDB:
    """
    Loads the files to the session, making easier to query to the files.
    """
    session = spark_session
    tables_file_type: FILE_TYPES = 'csv'
    has_header: bool = True
    db_path: Path | str = './db'
    table_name_in_folder: bool = True
    table_name_idx: int = 2
    export_queries_to: Path | str = './queries'


    def __post_init__(self):
        """
        Creates the tables on the session based on the self attributes 
        """
        self.files: tuple = get_files_from_root(self.db_path, self.tables_file_type)

        if not os.path.exists(self.db_path):
            raise IOError(f'DB folder does not exist: `{self.db_path}`')
        
        if not os.path.exists(self.export_queries_to):
            os.mkdir(self.export_queries_to)

        if self.table_name_in_folder:
            self.tables = tuple(
                [
                    file_path.split('/')[2] for file_path in self.files
                ]
            )
        else:
            self.tables = tuple(
                [
                    db_file.replace(
                        f'.{self.tables_file_type}', ''
                        ).upper() for db_file in self.files 
                ]
            )

        if self.tables_file_type == 'csv':

            for table, table_file in zip(self.tables, self.files):
                sdf = self.session.read.csv(table_file, header = self.has_header)
                sdf.createOrReplaceTempView(table)

        if self.tables_file_type == 'xlsx':
            for table, table_file in zip(self.tables, self.files):
                # Read the data
                df = pd.read_excel(table_file)

                # EXPORT THE DATA
                # We already have a readable file, so lets compress it
                # And make it faster to load. Also, making a scenario of
                # Do it just one time
                parquet_file = table_file.replace('xlsx', 'parquet')
                df.to_parquet(parquet_file)

                # Load the Data to the Spark Session
                sdf = self.read.parquet(parquet_file)
                sdf.createOrReplaceTempView(table)

    def query(
        self, sql:str,
        save: bool = False,
        name: str = None,
        sheet_name: str = None,
        show: int | None = 20):
        """
        Query and saves it if wanted
        """

        sdf = self.session.sql(sql)

        if show is not None:
            sdf.show(show)

        if save:
            if sheet_name is None:
                sheet_name = name
            df = ps.DataFrame(sdf)
            df.to_excel(
                f"{self.export_queries_to}/{name}.xlsx",
                index = False,
                sheet_name = sheet_name)
        return sdf


Spark = SparkDB()