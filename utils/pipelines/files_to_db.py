"""
Remember that you will need:
- A Table already created at the DataBase

"""
import os
from pathlib import Path

import pandas as pd

from load.to_sql import insert_many_from
from extract.files import get_table
from utils.file_manager import get_table

def update_db_from_file(
    file_name: Path | str, table: str,
    folder: Path | str = None, sheet_name: str | list | tuple = None) -> None:
    """

    """
    
    if folder is not None:
        full_file = f"{folder}/{file_name}"
    else:
        full_file = file_name

    table = get_table(full_file)


    