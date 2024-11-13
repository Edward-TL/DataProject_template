"""
Functions that helps in little common tasks
"""

from pathlib import Path
import shutil
from os import listdir
from os.path import exists as path_exists
from os import remove as os_remove

from dotenv import dotenv_values

import pandas as pd

def parent_dir() -> str:
    """Return the parent directory, so the folder can be added as needed"""
    return str(Path("./").parent.absolute())

def get_config(path: str) -> dict:
    """Given the path of the .env file, file return the values as a dictionary"""
    return dotenv_values(path)


def get_files(path: str, file_extension: str = '.xlsx', drop_if_contains:str | None = None) -> list[str]:
    """
    From a path, returns only the files that ends with `file_end`, like:
    '.parquet', '.xlsx', '.png', '.env', '.csv'

    If drop_if_contains is not `None`, exclude the file if contains the text passed.
    """

    # Grants a file extension and that path exists
    if (
        (file_extension.startswith('.')) or ('.' in file_extension)
        ) and (
            path_exists(path)
            ):

        # Exclude some cases
        if drop_if_contains is not None:
            return [
                file for file in listdir(path)\
                    if (
                        file.endswith(file_extension)
                        ) and (
                            drop_if_contains not in file
                            )
                    ]
        # Return everything
        return [file for file in listdir(path) if file.endswith(file_extension)]
    
    if '.' not in file_extension:
        raise ValueError(f"{file_extension} is not a file extension. Check the the value and the missing '.'")
    
    if path_exists(path) is False:
        raise ValueError(f"Path: `{path}` does not exist")



def df_mapper(df) -> dict:
    """Creates a python dictionary from a pandas DataFrame, so it can be
    used on a mapper function, like renaming columns."""

    mapper = df.to_dict(orient = "split")
    return {pair[0]:pair[1] for pair in mapper['data']}

def flat_list(matrix:list):
    """Flats a list of lists (matrix)"""
    return [item for row in matrix for item in row]


def gen_conditional_iter(col: str = None, df: pd.DataFrame = None, condition = None) -> tuple:
    """
    Creates a sorted tuple of the unique values from a given
    pandas DataFrame and the selected col. If a condition is
    given, returns the same value if it is an iterable, otherwise
    returns the element on a list. 
    """
    if df is not None:
        if condition is None:
            return tuple(df[col].sort_values().unique())

    if isinstance(condition, (list, tuple)):
        return condition

    return [condition]

def copy_directory(source_path, destination_path):
    """
    If destintation_path exists, will delete all and copy the
    source_path into destintation_path
    """
    if path_exists(destination_path):
        os_remove(destination_path)
    shutil.copytree(source_path, destination_path)

    
# if __name__ == "__main__":
    # print(MAIN_DIR)