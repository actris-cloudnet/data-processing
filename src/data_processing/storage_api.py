"""Metadata API for Cloudnet files."""
from typing import Tuple
from os import path
import requests
from data_processing import utils


class StorageApi:
    """Class for uploading / downloading files from the Cloudnet data archive in Sodankylä."""

    def __init__(self, url: str, auth: tuple, session=requests.Session()):
        self.url = url
        self.session = session
        self.auth = auth

    def download_raw_files(self, metadata: list,
                           dir_name: str,
                           uploaded_only: bool = True) -> Tuple[list, list]:
        """From a list of upload-metadata, download files."""
        full_paths = []
        checksums = []
        for row in metadata:
            if (uploaded_only and row['status'] == 'uploaded') or not uploaded_only:
                url = path.join(self.url, 'cloudnet-upload', row['s3Key'])
                res = requests.get(url, auth=self.auth)
                if res.status_code == 200:
                    full_path = path.join(dir_name, row['filename'])
                    with open(full_path, 'wb') as f:
                        f.write(res.content)
                    full_paths.append(full_path)
                    checksums.append(row['checksum'])
        if len(full_paths) == 0:
            raise ValueError
        return full_paths, checksums

    def download_raw_model_files(self, metadata: list,
                                 dir_name: str,
                                 uploaded_only: bool = True) -> Tuple[list, list]:
        """From a list of upload-metadata, download files."""
        full_paths = []
        checksums = []
        for row in metadata:
            if (uploaded_only and row['status'] == 'uploaded') or not uploaded_only:
                url = path.join(self.url, 'cloudnet-upload', row['s3Key'])
                res = requests.get(url, auth=self.auth)
                if res.status_code == 200:
                    full_path = path.join(dir_name, row['filename'])
                    with open(full_path, 'wb') as f:
                        f.write(res.content)
                    full_paths.append(full_path)
                    checksums.append(row['checksum'])
        if len(full_paths) == 0:
            raise ValueError
        return full_paths, checksums

    def upload_product(self, full_path: str, uuid: str):
        """Upload a processed Cloudnet file."""
        checksum = utils.md5sum(full_path, is_base64=True)
        headers = {'content-md5': checksum}
        url = path.join(self.url, 'cloudnet-product', uuid)  # What key would be good?
        res = requests.put(url, data=open(full_path, 'rb'), headers=headers, auth=self.auth)
        res.raise_for_status()