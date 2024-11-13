"""
Google API tools needed
"""
import os
import json
import mimetypes

from dataclasses import dataclass
import requests
from typing import Optional

from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

import gspread


from config_env import get_env
from helpers import get_file_size

general_envs = get_env(context_dict = True)

class GoogleDrive:
    def __init__(self, credentials):
        self.service = build(
            'drive', 'v3', credentials = credentials
            )

    def __post_init__(self):
        self.files = self.service.files()

    def create_folder(self, folder_name, parent_folder_id: str = None) -> str:
        """Create a folder in Google Drive and return its ID."""
        
        folder_metadata = {
            'name': folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            'parents': [parent_folder_id] if parent_folder_id else []
        }

        created_folder = self.service.files().create(
            body=folder_metadata,
            fields='id'
        ).execute()

        print(f'Created Folder ID: {created_folder["id"]}')
        return created_folder["id"]

    def list_folder(self, parent_folder_id=None, delete=False):
        """List folders and files in Google Drive."""
        results = self.service.files().list(
            q=f"'{parent_folder_id}' in parents and trashed=false" if parent_folder_id else None,
            pageSize=1000,
            fields="nextPageToken, files(id, name, mimeType)"
        ).execute()
        items = results.get('files', [])

        if not items:
            print("No folders or files found in Google Drive.")
        else:
            print("Folders and files in Google Drive:")
            for item in items:
                print(f"Name: {item['name']}, ID: {item['id']}, Type: {item['mimeType']}")
                if delete:
                    self.delete_files(item['id'])

    def delete_files(self, file_or_folder_id):
        """Delete a file or folder in Google Drive by ID."""
        try:
            self.files.delete(fileId=file_or_folder_id).execute()
            print(f"Successfully deleted file/folder with ID: {file_or_folder_id}")
        except Exception as e:
            print(f"Error deleting file/folder with ID: {file_or_folder_id}")
            print(f"Error details: {str(e)}")

    def download_file(self, file_id: str, file_name: str) -> tuple[requests.Response, Optional[str]]:
        """
        Downloads a file from Google Drive.

        Args:
            file_id (str): The ID of the file to download.
            file_name (str): The name to save the downloaded file as.

        Returns:
            Tuple[requests.Response, Optional[str]]: A tuple containing the response and the local file path,
            or (response, None) if download failed.
        """
        try:
            response = requests.get(
                f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media",
                headers=self.headers
            )
            if response.status_code == 200:
                file_path = os.path.join(os.getcwd(), file_name)
                with open(file_path, "wb") as f:
                    f.write(response.content)
                return response, file_path
            else:
                return response, None
        except Exception as e:
            return e, None

    def upload_file(self, file_name: str, file_path: str, drive_folder_id: str) -> requests.Response:
        """
        Insert new file.
        Returns : Id's of the file uploaded

        Load pre-authorized user credentials from the environment.
        TODO(developer) - See https://developers.google.com/identity
        for guides on implementing OAuth2 for the application.
        """

        try:
            complete_file_name = f"{file_path}/{file_name}"
            if not os.path.exists(complete_file_name):
                raise IOError(f"File does not exist: {complete_file_name}")
            # create drive api client

            file_metadata = {
                "name": file_name,
                'parents': [drive_folder_id],
                }
                
            file_type = mimetypes.guess_type(file_name)[0]

            media = MediaFileUpload(complete_file_name, mimetype = file_type)
            print(get_file_size(complete_file_name))
            file = (
                self.service.files()
                .create(
                    body = file_metadata,
                    media_body = media,
                    fields = "id"
                )
                .execute()
            )

            file_id = file.get('id')
            print(f'File ID: {file_id}')
            return file_id

        except HttpError as error:
            print(f"An error occurred: {error}")
            file = None

        return file.get("id")

@dataclass
class GoogleEnv:
    env_vals = general_envs['GOOGLE']
    scopes: tuple = tuple(
        [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
    "https://www.googleapis.com/auth/spreadsheets.readonly"
    ]
    )

    def __post_init__(self):
        self.credentials = (
            service_account
            .Credentials
            .from_service_account_info(
                self.env_vals)
                )
        # Set scopes
        self.creds_with_scope = self.credentials.with_scopes(self.scopes)
        self.sheets = gspread.authorize(self.creds_with_scope)
    
    def sheets_client(self):
        """Get client for Google Spread Sheet"""
        self.sheets = gspread.authorize(self.creds_with_scope)
        return self.sheets

    def drive_service(self):
        self.drive = GoogleDrive(self.creds_with_scope)

        return self.drive

Google = GoogleEnv()