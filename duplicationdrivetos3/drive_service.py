import io
import pickle
import os

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from .database_service import database
from .s3_service import s3
from kleenlogger import kleenlogger

# Google Scopes
SCOPES = ['https://www.googleapis.com/auth/drive']


class DriveService:
    def __init__(self):
        self.service = None

    def init_service(self):
        kleenlogger.logger.info('Initializing drive service')
        creds = None
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
        self.service = build('drive', 'v3', credentials=creds)
        kleenlogger.logger.info('Drive service initialization complete')

    def list_items(self, folder_id, page_token, resume, path):
        folder_id = "root" if folder_id == "my-drive" else folder_id
        kleenlogger.logger.info(
            'Listing items in folder {} with pagetoken {} at {}'.format(folder_id, page_token, path)
        )
        try:
            results = self.service.files().list(
                pageSize=1000,
                pageToken=page_token,
                fields="nextPageToken, files(id, name, mimeType, size)",
                q="'{}' in parents and trashed=false".format(folder_id)
            ).execute()
        except HttpError as err:
            print(err)
            kleenlogger.logger.error(err.content)
            drive.init_service()
            try:
                results = self.service.files().list(
                    pageSize=1000,
                    pageToken=page_token,
                    fields="nextPageToken, files(id, name, mimeType, size)",
                    q="'{}' in parents and trashed=false".format(folder_id)
                ).execute()
            except HttpError as err2:
                print(err2)
                kleenlogger.logger.error(err2.content)
                kleenlogger.logger.error('Script exiting on fatal error')
                exit()
        except Exception as err:
            print(err)
            kleenlogger.logger.error(err)
            kleenlogger.logger.error('Script exiting on fatal unhandle error')
            exit()
        items = results.get('files', [])
        for item in items:
            filename_override = False
            if resume:
                if database.is_uploaded(item['id']):
                    continue
            print("File name : {}, File type : {}, File id : {}".format(item['name'], item['mimeType'], item['id']))
            out_format = DriveService.is_google_format(item['mimeType'])
            if item['mimeType'] == "application/vnd.google-apps.folder":
                kleenlogger.logger.info('Found a folder with id {} and name {}'.format(item['id'], item['name']))
                self.list_items(item['id'], None, resume, path + item['name'] + "/")
                continue
            elif out_format == "skip":
                kleenlogger.logger.warn(
                    'Skipping file {} with name {} because not handle by export methods mimeType={}'.format(
                        item['id'],
                        item['name'],
                        item['mimeType']
                    )
                )
                continue
            elif out_format is not False:
                buffer = self.get_file_google(item['id'], out_format[0])
                filename_override = item['name'] + "." + out_format[1]
            else:
                buffer = self.get_file(item['id'])
            try:
                filename = filename_override if filename_override is not False else item['name']
                s3.upload_to_s3(filename, path, buffer.getvalue())
                database.update_row(item['id'])
                kleenlogger.logger.info('File {} uploaded successfully to S3'.format(item['id']))
            except Exception as err:
                print(err)
                kleenlogger.logger.error(err)
                kleenlogger.logger.error('Script exiting on fatal unhandle error')
                exit()
            buffer.flush()
            buffer.close()
        page_token = results.get('nextPageToken', None)
        if page_token is not None:
            kleenlogger.logger.info('Found nextPageToken, running list_items')
            self.list_items(folder_id, page_token, resume, path)

    def get_file_google(self, file_id, mime_type):
        kleenlogger.logger.info('Getting file {} as google file with the mimeType {}'.format(file_id, mime_type))
        try:
            request = self.service.files().export_media(fileId=file_id, mimeType=mime_type)
        except HttpError as err:
            print(err)
            kleenlogger.logger.error(err.content)
            drive.init_service()
            try:
                request = self.service.files().export_media(fileId=file_id, mimeType=mime_type)
            except HttpError as err2:
                print(err2)
                kleenlogger.logger.error(err2.content)
                kleenlogger.logger.error('Script exiting on fatal error')
                exit()
        except Exception as err:
            print(err)
            kleenlogger.logger.error(err)
            kleenlogger.logger.error('Script exiting on fatal unhandle error')
            exit()
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            try:
                status, done = downloader.next_chunk()
                print("Download %d%%." % int(status.progress() * 100))
            except Exception as e:
                print(e)
                exit()
        database.insert_row(file_id, fh.getbuffer().nbytes)
        kleenlogger.logger.info('File {} downloaded successfully'.format(file_id))
        return fh

    def get_file(self, file_id):
        kleenlogger.logger.info('Getting file {} as binary file'.format(file_id))
        try:
            request = self.service.files().get_media(fileId=file_id)
        except HttpError as err:
            print(err)
            kleenlogger.logger.error(err.content)
            drive.init_service()
            try:
                request = self.service.files().get_media(fileId=file_id)
            except HttpError as err2:
                print(err2)
                kleenlogger.logger.error(err2.content)
                kleenlogger.logger.error('Script exiting on fatal error')
                exit()
        except Exception as err:
            print(err)
            kleenlogger.logger.error(err)
            kleenlogger.logger.error('Script exiting on fatal unhandle error')
            exit()
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            try:
                status, done = downloader.next_chunk()
                print("Download %d%%." % int(status.progress() * 100))
            except Exception as e:
                print(e)
                exit()
        database.insert_row(file_id, fh.getbuffer().nbytes)
        kleenlogger.logger.info('File {} downloaded successfully'.format(file_id))
        return fh

    @staticmethod
    def is_google_format(mimetype: str):
        supported = [
            "application/vnd.google-apps.document",
            "application/vnd.google-apps.drawing",
            "application/vnd.google-apps.presentation",
            "application/vnd.google-apps.spreadsheet"
        ]
        convert_supported = [
            ["application/vnd.openxmlformats-officedocument.wordprocessingml.document", "docx"],
            ["image/png", "png"],
            ["application/vnd.openxmlformats-officedocument.presentationml.presentation", "pptx"],
            ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "xlsx"]
        ]
        if mimetype in supported:
            return convert_supported[supported.index(mimetype)]
        elif 'application/vnd.google-apps' in mimetype:
            return "skip"
        else:
            return False

    @staticmethod
    def partial(total_byte_len, part_size_limit):
        s = []
        for p in range(0, total_byte_len, part_size_limit):
            last = min(total_byte_len - 1, p + part_size_limit - 1)
            s.append([p, last])
        return s


drive = DriveService()
