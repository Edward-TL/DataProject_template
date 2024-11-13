"""
Gspread docs for any doubt: https://docs.gspread.org/en/latest/user-guide.html

"""
from google.oauth2 import service_account
import pandas as pd
import gspread

from config_env import get_env

general_envs = get_env(context_dict = True)
service_account_info = general_envs['GOOGLE']

scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
    "https://www.googleapis.com/auth/spreadsheets.readonly"
    ]

def get_spreadsheet(
        sheets_url: str | None = None,
        sheets_key: str | None = None,
        service_account_info: dict = service_account_info) -> gspread.Client:
    """
    Returns Google's equivalent of Excel WorkBOOKs
    """
    if (sheets_url is None) and (sheets_key is None):
        raise ValueError('Must give a sheets_url or sheets_key. Both are None')
    
    if (sheets_url is not None) and (sheets_key is not None):
        raise ValueError(
            'Choose between sheets_url or sheets_key. Both are given and this could lead to misunderstandings'
            )
    
    # Get credentials from GCP
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    # Set scopes
    creds_with_scope = credentials.with_scopes(scope)
    # Get client for Google Spread Sheet
    client = gspread.authorize(creds_with_scope)
    # Get SpreadSheet Data
    if sheets_url is not None:
        spreadsheet = client.open_by_url(sheets_url)

    if sheets_key is not None:
        spreadsheet = client.open_by_key(sheets_url)

    return spreadsheet

def to_df(worksheet: gspread.Worksheet) -> pd.DataFrame:
    return pd.DataFrame(worksheet.get_all_records())

def get_df_from_spreadsheet(
        sheets_url: str | None = None,
        sheets_key: str | None = None,
        service_account_info: dict = service_account_info,
        ws_title: str | None = None,
        worksheet_idx: int = 1) -> pd.DataFrame:
    

    sh = get_spreadsheet(
        sheets_url,
        sheets_key,
        service_account_info
        )

    # Get worksheet
    if ws_title is not None:
        worksheet = sh.get_worksheet(ws_title)
    else:
        worksheet = sh.get_worksheet(worksheet_idx)
        
    return to_df(worksheet)
    
def get_all_dfs_from_spreadsheet(
        sheets_url: str | None = None,
        sheets_key: str | None = None,
        service_account_info: dict = service_account_info) -> dict[str: pd.DataFrame]:

    sh = get_spreadsheet(
        sheets_url,
        sheets_key,
        service_account_info
        )

    # Get worksheet
    return {
        sheet: to_df(sh.get_worksheet(sheet)) for sheet in sh.worksheets()
        }