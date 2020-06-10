#!../prod_venv/bin/python3
import os
import glob
import shutil
import argparse
from tqdm import tqdm
from operational_processing import metadata_api
import operational_processing.utils as process_utils


fix_attributes = __import__('transition_scripts').fix_attributes


def main():

    config = process_utils.read_conf(ARGS)
    md_api = metadata_api.MetadataApi(config['main']['METADATASERVER']['url'])

    paths = _build_data_paths()

    print('Reading files...')
    files = _get_files(paths)

    print('Synchronizing directories:')
    _sync_dirs(files, paths)


def _build_data_paths():
    return {key: '/'.join((getattr(ARGS, key), ARGS.site[0], 'calibrated'))
            for key in ('input', 'output')}


def _get_files(paths):
    return glob.glob(f"{paths['input']}/**/{ARGS.folder}/**/*.nc", recursive=True)


def _sync_dirs(files, paths):
    for file in tqdm(files):
        target_file = _create_target_name(file, paths)
        if not os.path.isfile(target_file):
            if ARGS.dry_run:
                print(file, target_file)
            else:
                _deliver_file(file, target_file)


def _create_target_name(file, paths):
    return file.replace(paths['input'], paths['output'])


def _deliver_file(source_file, target_file):
    os.makedirs(os.path.dirname(target_file), exist_ok=True)
    shutil.copyfile(source_file, target_file)
    uuid = fix_attributes(target_file, overwrite=False)
    md_api.put(uuid, target_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fix model files for data portal. Example: '
                                                 'scripts/sync-folders.py bucharest ecmwf')
    parser.add_argument('site', nargs='+', metavar='SITE', help='Site Name')
    parser.add_argument('folder', metavar='FOO', help='Name of model folder, e.g., ecmwf')
    parser.add_argument('--input', metavar='/path/to/',
                        help='Input directory. Default is /ibrix/arch/dmz/cloudnet/data',
                        default='/ibrix/arch/dmz/cloudnet/data')
    parser.add_argument('--output', metavar='/path/to/',
                        help='Output directory. Default is /data/clouddata/sites/',
                        default='/data/clouddata/sites')
    parser.add_argument('-d', '--dry', dest='dry_run', action='store_true',
                        help='Dry run the script for testing the behaviour without writing any files.', default=False)

    ARGS = parser.parse_args()
    main()
