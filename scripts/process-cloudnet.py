#!/usr/bin/env python3
"""Master script for CloudnetPy processing."""
import os
import argparse
from typing import Tuple
import shutil
from tempfile import TemporaryDirectory
from tempfile import NamedTemporaryFile
from cloudnetpy.instruments import rpg2nc, ceilo2nc, mira2nc
from cloudnetpy.utils import date_range
from data_processing import utils
from data_processing.metadata_api import MetadataApi
from data_processing.storage_api import StorageApi
from data_processing import concat_lib
from data_processing import modifier


def main():
    """The main function."""

    config = utils.read_main_conf(ARGS)
    site_meta = utils.read_site_info(ARGS.site[0])
    start_date = utils.date_string_to_date(ARGS.start)
    stop_date = utils.date_string_to_date(ARGS.stop)

    md_api = MetadataApi(config['METADATASERVER']['url'])
    storage_api = StorageApi(config['STORAGE-SERVICE']['url'],
                             (config['STORAGE-SERVICE']['username'],
                              config['STORAGE-SERVICE']['password']),
                             product_bucket=_get_product_bucket())

    for date in date_range(start_date, stop_date):
        date_str = date.strftime("%Y-%m-%d")
        print(f'{date_str}')
        process = Process(site_meta, date_str, md_api, storage_api)
        for processing_type in utils.get_raw_processing_types():
            try:
                getattr(process, f'process_{processing_type}')(processing_type)
            except AttributeError:
                continue
            except InputFileMissing:
                print(f'No raw {processing_type} data or already processed.')


def _get_product_bucket() -> str:
    return 'cloudnet-product' if ARGS.new_version else 'cloudnet-product-volatile'


class Process:
    def __init__(self, site_meta: dict, date_str: str, md_api: MetadataApi, storage_api: StorageApi):
        self.site_meta = site_meta
        self.date_str = date_str  # YYYY-MM-DD
        self.md_api = md_api
        self.storage_api = storage_api

    def process_mwr(self, instrument_type: str):
        """Process Cloudnet mwr file"""
        try:
            raw_daily_file = NamedTemporaryFile(suffix='.nc')
            valid_checksums, original_filename = self._get_daily_raw_file(raw_daily_file.name, 'hatpro')
        except ValueError:
            raise InputFileMissing(f'Raw {instrument_type}')
        uuid = modifier.fix_mwr_file(raw_daily_file.name, original_filename, self.date_str, self.site_meta['name'])
        self._upload_data_and_metadata(raw_daily_file.name, uuid, valid_checksums, instrument_type)

    def process_model(self, instrument_type: str):
        """Process Cloudnet model file"""
        try:
            raw_daily_file = NamedTemporaryFile(suffix='.nc')
            valid_checksums, _ = self._get_daily_raw_file(raw_daily_file.name)
        except ValueError:
            raise InputFileMissing(f'Raw {instrument_type}')
        uuid = modifier.fix_model_file(raw_daily_file.name)
        self._upload_data_and_metadata(raw_daily_file.name, uuid, valid_checksums, instrument_type)

    def process_lidar(self, instrument_type: str):
        """Process Cloudnet lidar file."""
        try:
            raw_daily_file = NamedTemporaryFile(suffix='.nc')
            valid_checksums = self._concatenate_chm15k(raw_daily_file.name)
        except ValueError:
            try:
                raw_daily_file = NamedTemporaryFile(suffix='.DAT')
                valid_checksums, _ = self._get_daily_raw_file(raw_daily_file.name, 'cl51')
            except ValueError:
                raise InputFileMissing(f'Raw {instrument_type}')

        lidar_file = NamedTemporaryFile()
        uuid = ceilo2nc(raw_daily_file.name, lidar_file.name, site_meta=self.site_meta)
        self._upload_data_and_metadata(lidar_file.name, uuid, valid_checksums, instrument_type)

    def process_radar(self, instrument_type: str):
        """Process Cloudnet radar file."""
        radar_file = NamedTemporaryFile()
        try:
            temp_dir = TemporaryDirectory()
            full_paths, valid_checksums = self._download_data(temp_dir, 'rpg-fmcw-94')
            uuid = rpg2nc(temp_dir.name, radar_file.name, site_meta=self.site_meta)
        except ValueError:
            try:
                raw_daily_file = NamedTemporaryFile(suffix='.mmclx')
                valid_checksums, _ = self._get_daily_raw_file(raw_daily_file.name, 'mira')
                uuid = mira2nc(raw_daily_file.name, radar_file.name, site_meta=self.site_meta)
            except ValueError:
                raise InputFileMissing(f'Raw {instrument_type}')
        self._upload_data_and_metadata(radar_file.name, uuid, valid_checksums, instrument_type)

    def _concatenate_chm15k(self, raw_daily_file: str) -> list:
        """Concatenate several chm15k files into one file for certain site / date."""
        temp_dir = TemporaryDirectory()
        full_paths, checksums = self._download_data(temp_dir, 'chm15k')
        valid_full_paths = concat_lib.concat_chm15k_files(full_paths, self.date_str, raw_daily_file)
        return [checksum for checksum, full_path in zip(checksums, full_paths) if full_path in valid_full_paths]

    def _get_daily_raw_file(self, raw_daily_file: str, instrument: str = None) -> Tuple[list, str]:
        """Downloads and saves to /tmp a single daily instrument or model file."""
        temp_dir = TemporaryDirectory()
        full_paths, checksums = self._download_data(temp_dir, instrument)
        full_path = full_paths[0]
        shutil.move(full_path, raw_daily_file)
        original_filename = os.path.basename(full_path)
        return checksums, original_filename

    def _download_data(self, temp_dir: TemporaryDirectory, instrument: str = None) -> Tuple[list, list]:
        if instrument == 'hatpro':
            all_metadata_for_day = self.md_api.get_uploaded_metadata(self.site_meta['id'], self.date_str, instrument)
            metadata = [row for row in all_metadata_for_day if row['filename'].lower().endswith('.lwp.nc')]
        elif instrument:
            metadata = self.md_api.get_uploaded_metadata(self.site_meta['id'], self.date_str, instrument=instrument)
        else:
            all_metadata_for_day = self.md_api.get_uploaded_metadata(self.site_meta['id'], self.date_str)
            metadata = self._select_optimum_model(all_metadata_for_day)
        return self.storage_api.download_raw_files(metadata, temp_dir.name)

    @staticmethod
    def _select_optimum_model(all_metadata_for_day: list) -> list:
        model_metadata = [row for row in all_metadata_for_day if row['model']]
        sorted_metadata = sorted(model_metadata, key=lambda k: k['model']['optimumOrder'])
        return [sorted_metadata[0]] if sorted_metadata else []

    def _upload_data_and_metadata(self, full_path: str, uuid, valid_checksums: list, product: str) -> None:
        self.md_api.put(uuid, full_path)
        self.storage_api.upload_product(full_path, self._get_product_key(product))
        self._update_statuses(valid_checksums)

    def _get_product_key(self, product: str) -> str:
        return f"{self.date_str.replace('-', '')}_{self.site_meta['id']}_{product}.nc"

    def _update_statuses(self, checksums: list) -> None:
        for checksum in checksums:
            self.md_api.change_status_from_uploaded_to_processed(checksum)


class InputFileMissing(Exception):
    """Internal exception class."""
    def __init__(self, file_type: str):
        self.message = f'{file_type} file missing'
        super().__init__(self.message)


class CategorizeFileMissing(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'Categorize file missing'
        super().__init__(self.message)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Cloudnet data.')
    parser.add_argument('site', nargs='+', help='Site Name',
                        choices=['bucharest', 'norunda', 'granada', 'mace-head'])
    parser.add_argument('--config-dir', dest='config_dir', type=str, metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')
    parser.add_argument('--start', type=str, metavar='YYYY-MM-DD',
                        help='Starting date. Default is current day - 7.',
                        default=utils.get_date_from_past(7))
    parser.add_argument('--stop', type=str, metavar='YYYY-MM-DD',
                        help='Stopping date. Default is current day - 1.',
                        default=utils.get_date_from_past(1))
    parser.add_argument('-na', '--no-api', dest='no_api', action='store_true',
                        help='Disable API calls. Useful for testing.', default=False)
    parser.add_argument('--new-version', dest='new_version', action='store_true',
                        help='Process new version.', default=False)
    ARGS = parser.parse_args()
    main()
