
"""
Insert the project to sys.path, and returns the standard
work_path for project development, including the .env values
"""
import sys
# from os.path import expanduser
# from os import getenv
from pathlib import Path
from dotenv import dotenv_values
from helpers import get_files

from utils.logger import logger

root_path = str(
        Path(__file__).resolve().parent.parent
        )

project_path = str(
    Path(__file__).resolve().parent
    )

def add_project_path(info_logger: bool = False, print_path: bool = False) -> None:
    """
    CONFIG FROM ROOT. Will temporary add project's path into sys.path, so you can
    work as it was an installed library
    """
    

    sys.path.append(root_path)
    msg = f"{root_path} appended on sys"
    if info_logger:
        logger.info(root_path)
    if print_path:
        print(msg)

def get_env(context_dict: bool = False, testing: bool = False):
    """
    Extrae la información del .env para la generación de las rutas necesarias
    para la compilación, limpieza y carga de la bitácora
    """

    if testing:
        env_extension = '.env.example'
        env_path = 'env_example'
    else:
        env_extension = '.env'
        env_path = 'env'

    env_dir = f'{project_path}/{env_path}'
    env_files = get_files(env_dir, env_extension)
    
    if testing:
        print(env_dir)
        print(env_files)

    envs_as_dictionaries = {
        file.replace(env_extension, '').upper(): dotenv_values(f'{env_dir}/{file}')\
            for file in env_files
        }
    
    if context_dict:
        return envs_as_dictionaries

    flat_data = {}
    for env_type, env_data in envs_as_dictionaries.items():
        for k, v in env_data.items():
            flat_data[f'{env_type}_{k}'] = v

    return flat_data

if __name__ == '__main__':
    print(root_path)
    print(project_path)
    
    # print("SYS.PATH:")
    # for element in sys.path:
    #     print(element)

    CONTEXT_ENV = get_env(context_dict=True, testing=True)
    print(CONTEXT_ENV)
    print(CONTEXT_ENV['SQL']['USER'])

    FLAT_ENV = get_env()
    print(FLAT_ENV)