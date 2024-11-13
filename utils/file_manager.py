


from pathlib import Path
import shutil
from os import listdir
from typing import Literal
import time

from os.path import isdir, getctime, getmtime, getsize
from os.path import exists as path_exists
from os import remove as os_remove


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
