"""
Manage to get all files, and the tools needed to improve its extraction.
If needed, can create a table that considers the creation and last modification
of the files, to consider this when reading, concatenating or choosing which file
is required
"""

from pathlib import Path
import shutil
from os import listdir
from typing import Literal
import time
import csv

from os.path import isdir, getctime, getmtime, getsize
from os.path import exists as path_exists
from os import remove as os_remove

import pandas as pd
from helpers import flat_list, get_files, FILE_TYPES

def copy_directory(source_path, destination_path):
    """
    If destintation_path exists, will delete all and copy the
    source_path into destintation_path
    """
    if path_exists(destination_path):
        os_remove(destination_path)
    shutil.copytree(source_path, destination_path)


def get_directories(path:str) -> list[str]:
    """Get all directories in path"""
    return [d for d in listdir(path) if isdir(f"{path}/{d}")]

def get_modification_datetime(files_dir:str, child_files:list) -> list:
    """
    Get (according to function's name) the creation date

    Return the system’s ctime which, on some systems (like Unix) is
    the time of the last metadata change, and, on others (like Windows),
    is the creation time for path. The return value is a number giving the
    number of seconds since the epoch (see the time module).
    Raise OSError if the file does not exist or is inaccessible.

    RIGHT NOW IT SEEMS TO RETURN THE LAST METADATA CHANGE
    
    https://docs.python.org/3/library/os.path.html#os.path.getctime
    """
    return [
        time.ctime(getctime(f"{files_dir}/{xl}")) for xl in child_files
        ]

def get_creation_datetime(files_dir:str, child_files:list) -> list:
    """
    Get (according to function's name) the last modification date
    
    Return the time of last modification of path. The return value
    is a floating-point number giving the number of seconds since
    the epoch (see the time module).

    SEEMS LIKE THE LAST MODIFICATION OF PATH ITS THE LAST TIME THAT
    THE FILE WAS CREATED
    
    Raise OSError if the file does not exist or is inaccessible.
    https://docs.python.org/3/library/os.path.html#os.path.getmtime
    """
    return [
        time.ctime(getmtime(f"{files_dir}/{xl}")) for xl in child_files
        ]

def get_file_size(file: Path | str, s: Literal['b', 'kb', 'mb'] = 'b') -> list:
    """
    Get files sizes, according to the scale [s] choiced: b, kb, mb
    """
    size = {
        'b': 1,
        'kb' : 1024 ** 1,
        'mb' : 1024 ** 2,
        'gb' : 1024 ** 3
    }
    return round(getsize(file) / size[s], 2)

def get_files_sizes(files_dir:str, child_files:list, s: Literal['b', 'kb', 'mb'] = 'kb') -> list:
    """
    Get files sizes, according to the scale [s] choiced: b, kb, mb
    """
    size = {
        'b': 1,
        'kb' : 1024 ** 1,
        'mb' : 1024 ** 2,
        'gb' : 1024 ** 3
    }
    return [
        round(getsize(f"{files_dir}/{xl}")/size[s],2) for xl in child_files

    ]

def get_files_from_root(
    root_path: Path | str, file_type: FILE_TYPES,
    shape: Literal['table', 'list'] = 'list') -> list | pd.DataFrame:
    """
    Bring files in root path and its childs.

    shape: if `table` returns a pd.DataFrame
    """

    if shape == 'list':
        files_found = [
                [
                    f"{root_path}/{folder}/{file_found}"\
                    for file_found in get_files(f"{root_path}/{folder}", file_type)
                ] for folder in get_directories(root_path)
            ]
        files_found.insert(0, [
                f'{{root_path}}/{file_found}' for file_found in get_files(
                    root_path, file_type
                    )
            ]
        )

        return flat_list(files_found)

    # if shape == 'table':
    children = get_directories(root_path)

    files_found = [
            get_files(f"{root_path}/{folder}", file_type) for folder in children
        ]
    paths = [
        [children[f]] * len(files_found) for f in range(len(children))
    ]

    root_files = get_files(root_path, file_type)
    files_found.insert(0,root_files)
    paths.insert(0, [root_path] * len(root_files))
            
    return pd.DataFrame(
        {
            'location': flat_list(paths),
            'file_name': flat_list(files_found)
        }
    )

def read_csv(file: Path | str) -> tuple:
    with open(file, 'r') as file:
        reader = csv.reader(file)
        return tuple(reader)


def get_table(file_path: Path | str) -> pd.DataFrame:
    """
    """
    file_extension = file_path.split('.')[-1]

    if file_extension == 'xlsx':
        return pd.read_excel(file_path)
        
    if file_extension == 'csv':
        return pd.read_csv(file_path)
        
    raise IOError(f'Required a valid file type. Got a {file_extension} type')
