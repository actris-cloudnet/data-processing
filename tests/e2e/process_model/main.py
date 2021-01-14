#!/usr/bin/env python3
import subprocess
from os import path
import sys
import argparse
import test_utils.utils as test_utils
from data_processing import utils
from tempfile import NamedTemporaryFile
import re
sys.path.append('scripts/')
process_cloudnet = __import__("process-cloudnet")

SCRIPT_PATH = path.dirname(path.realpath(__file__))
session, adapter, mock_addr = test_utils.init_test_session()


def register_storage_urls(temp_file):

    def save_product(request):
        with open(temp_file.name, mode='wb') as f:
            f.write(request.body.read())
        return True

    site = 'bucharest'

    data = [
        ('eb176ca3-374e-471c-9c82-fc9a45578883', f'20201022_{site}_ecmwf.nc'),
        ('80c2fab5-2dc5-4692-bafe-a7274071770e', f'20201022_{site}_gdas1.nc'),
    ]
    upload_bucket = utils.get_upload_bucket(site)
    for uuid, filename in data:
        url = f'{mock_addr}{upload_bucket}/{site}/{uuid}/{filename}'
        adapter.register_uri('GET', url, body=open(f'tests/data/raw/model/{filename}', 'rb'))

    product_bucket = utils.get_product_bucket(site, True)
    for _, filename in data:
        url = f'{mock_addr}{product_bucket}/{filename}'
        adapter.register_uri('PUT', url, additional_matcher=save_product,
                             json={'size': 667, 'version': 'abc'})
    # images:
    img_bucket = utils.get_image_bucket(site)
    adapter.register_uri('PUT', re.compile(f'{mock_addr}{img_bucket}/(.*?)'))


def main():
    test_utils.start_server(5000, 'tests/data/server/metadata/process_model', f'{SCRIPT_PATH}/md.log')

    # Processes new volatile files of ALL existing raw model files
    # (one of them has existing volatile file):
    _process()

    # Should work identically with -reprocess flag:
    _process(main_extra_args=('-r',))


def _process(main_extra_args=()):
    with open(f'{SCRIPT_PATH}/md.log', 'w'):
        pass
    args = ['bucharest', f"--config-dir=tests/data/config", f"--start=2020-10-22",
            f"--stop=2020-10-23", '-p=model']
    temp_file = NamedTemporaryFile()
    register_storage_urls(temp_file)
    std_args = test_utils.start_output_capturing()
    process_cloudnet.main(args + list(main_extra_args), storage_session=session)
    output = test_utils.reset_output(*std_args)
    try:
        subprocess.check_call(['pytest', '-v', f'{SCRIPT_PATH}/tests.py', '--output', output,
                               '--full_path', temp_file.name])
    except subprocess.CalledProcessError:
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cloudnet Classification processing e2e test.')
    ARGS = parser.parse_args()
    main()
