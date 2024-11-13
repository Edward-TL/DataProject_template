from pathlib import Path

from connections.sql import DB_URL
from helpers import get_files

# def raw_sql(query: str) -> list[tuple]:
#     """Creates the connection to the DataBase and returns
#     the query requested in a list of tuples"""
#     with DB_ENGINE.connect() as connection:
#         cursor = connection.execute(text(query))
#         query_tuples = cursor.fetchall()

#         return query_tuples

# def query(query: str) -> pd.DataFrame:
#     """Creates the connection to the DataBase and returns
#     the query requested in a pandas DataFrame object"""
#     return pd.DataFrame(raw_sql(query))

extract_folder_path = str(
    Path(__file__).resolve().parent
    )

def read_query_file(file: Path | str) -> str:
    with open(file, 'r') as sql_file:
        return sql_file.read()

def get_queries() -> dict:
    query_files = get_files(f'{extract_folder_path}/queries', file_type='sql')

    full_qf_paths = [f'{extract_folder_path}/queries/{query_file}' for query_file in query_files]

    return {
        query_file.replace('.sql', '') : read_query_file(full_qf_file)\
            for query_file, full_qf_file in zip(query_files, full_qf_paths)
    }

if __name__ == "__main__":
    print(DB_URL)
