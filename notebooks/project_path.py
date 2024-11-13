
"""
Insert the project to sys.path, and returns the standard
work_path for project development, including the .env values
"""
import sys
# from os.path import expanduser
# from os import getenv
from pathlib import Path
from utils.logger import logger


root_path = str(
    Path(__file__).resolve().parent.parent
    )

project_path = str(
    Path(__file__).resolve().parent.parent
    )
    
def add_project_path(info_logger: bool = False, print_path: bool = False) -> None:
    """
    Appends to sys.path the projects root path, so it could works as a
    third party installed module.

    If you want to see the appended path, choose if you want it as a logger or
    simple console print.

    Remember that here you are at a folder level, not in root/main level.
    """

    sys.path.append(root_path)
    msg = f"{root_path} appended on sys"
    if info_logger:
        logger.info(root_path)
    if print_path:
        print(msg)
