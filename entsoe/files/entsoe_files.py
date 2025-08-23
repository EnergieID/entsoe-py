from typing import IO, Optional, Dict
import requests
from entsoe import __version__
import pandas as pd
import json
from io import BytesIO
import zipfile
from .decorators import check_expired
import os

# DOCS for entsoe file library: https://transparencyplatform.zendesk.com/hc/en-us/articles/35960137882129-File-Library-Guide
# postman description: https://documenter.getpostman.com/view/28274243/2sB2qgfz3W


class EntsoeFileClient:
    BASEURL = "https://fms.tp.entsoe.eu/"

    def __init__(self, username: str = None, pwd: str = None, session: Optional[requests.Session] = None,
                 proxies: Optional[Dict] = None, timeout: Optional[int] = None
                 ):
        self.proxies = proxies
        self.timeout = timeout
        self.username = username
        if self.username is None:
            self.username = os.getenv('ENTSOE_USERNAME')
        self.pwd = pwd
        if self.pwd is None:
            self.pwd = os.getenv('ENTSOE_PWD')

        if self.username is None or self.pwd is None:
            raise Exception('username and password must be set')

        if session is None:
            session = requests.Session()
        self.session = session
        self.session.headers.update({
            'user-agent': f'entsoe-py {__version__} (github.com/EnergieID/entsoe-py)'
        })

        self.access_token = None
        self.expire = None

        self._update_token()

    def _update_token(self):
        # different url that other calls so hardcoded new one here
        r = self.session.post(
            'https://keycloak.tp.entsoe.eu/realms/tp/protocol/openid-connect/token', data={
                'client_id': 'tp-fms-public',
                'grant_type': 'password',
                'username': self.username,
                'password': self.pwd
            },
            proxies=self.proxies, timeout=self.timeout
        )
        r.raise_for_status()
        data = r.json()
        self.expire = pd.Timestamp.now(tz='europe/amsterdam') + pd.Timedelta(seconds=data['expires_in'])
        self.access_token = data['access_token']

    @check_expired
    def list_folder(self, folder: str) -> dict:
        """
        returns a dictionary of filename: unique file id
        """
        if not folder.endswith('/'):
            folder += '/'
        r = self.session.post(self.BASEURL + "listFolder",
                              data=json.dumps({
                                  "path": "/TP_export/" + folder,
                                  "sorterList": [
                                      {
                                          "key": "periodCovered.from",
                                          "ascending": True
                                      }
                                  ],
                                  "pageInfo": {
                                      "pageIndex": 0,
                                      "pageSize": 5000  # this should be enough for basically anything right now
                                  }
                              }),
                              headers={
                                  'Authorization': f'Bearer {self.access_token}',
                                  'Content-Type': 'application/json'
                              },
                              proxies=self.proxies, timeout=self.timeout)
        r.raise_for_status()
        data = r.json()
        return {x['name']: x['fileId'] for x in data['contentItemList']}

    @check_expired
    def download_single_file_raw(self, folder, filename) -> bytes:
        """
        download a file by filename, it is important to split folder and filename here.
        Returns the direct response from the ENTSO-E file api, which is in the format of a Zip file.
        """
        if not folder.endswith('/'):
            folder += '/'
        r = self.session.post(self.BASEURL + "downloadFileContent",
                              data=json.dumps({
                                  "folder": "/TP_export/" + folder,
                                  "filename": filename,
                                  "downloadAsZip": True,
                                  "topLevelFolder": "TP_export",
                              }),
                              headers={
                                  'Authorization': f'Bearer {self.access_token}',
                                  'Content-Type': 'application/json'
                              })
        r.raise_for_status()
        return r.content

    def download_single_file(self, folder, filename) -> pd.DataFrame:
        """
        download a file by filename, it is important to split folder and filename here.
        """
        content = self.download_single_file_raw(folder, filename)
        stream = BytesIO(content)
        stream.seek(0)
        zf = zipfile.ZipFile(stream)
        with zf.open(zf.filelist[0].filename) as file:
            return pd.read_csv(file, sep='\t', encoding='utf-8-sig')

    @check_expired
    def download_multiple_files_raw(self, file_ids: list) -> bytes:
        """
        for now when downloading multiple files only list of file ids is supported by this package
        """
        r = self.session.post(self.BASEURL + "downloadFileContent",
                              data=json.dumps({
                                  "fileIdList": file_ids,
                                  "downloadAsZip": True,
                                  "topLevelFolder": "TP_export",
                              }),
                              headers={
                                  'Authorization': f'Bearer {self.access_token}',
                                  'Content-Type': 'application/json'
                              },
                              proxies=self.proxies, timeout=self.timeout)
        r.raise_for_status()
        return r.content

    def download_multiple_files(self, file_ids: list) -> pd.DataFrame:
        content = self.download_multiple_files_raw(file_ids)
        stream = BytesIO(content)
        stream.seek(0)
        zf = zipfile.ZipFile(stream)
        df = []
        for fz in zf.filelist:
            with zf.open(fz.filename) as file:
                df.append(pd.read_csv(file, sep='\t', encoding='utf-8-sig'))

        return pd.concat(df)
