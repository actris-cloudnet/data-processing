#!venv/bin/python3
"""Script for plotting CloudnetPy images."""
import os
from pathlib import Path
import argparse
import importlib
from cloudnetpy.plotting import generate_figure
import netCDF4
process_utils = importlib.import_module("operational-processing").utils
file_paths = importlib.import_module("operational-processing").file_paths


def main():
    root = _get_root_path()
    for file in Path(root).rglob('*.nc'):
        if not _is_date_in_range(file):
            continue
        nc_file_name = str(file)
        image_name = nc_file_name.replace('nc', 'png')
        file_type = _get_file_type(nc_file_name)
        try:
            fields, max_alt = _get_fields_for_plot(file_type)
        except NotImplementedError:
            continue
        if _is_plottable(image_name):
            print(f"Plotting: {image_name}")
            generate_figure(nc_file_name, fields, show=False,
                            image_name=image_name, max_y=max_alt)


def _get_root_path():
    if ARGS.output:
        root = ARGS.output
    else:
        config = process_utils.read_conf(ARGS)
        root = config['main']['PATH']['output']
    return '/'.join((root, ARGS.site[0]))


def _is_plottable(image_name):
    if os.path.isfile(image_name) and not ARGS.overwrite:
        return False
    return True


def _get_file_type(nc_file_name):
    nc = netCDF4.Dataset(nc_file_name)
    file_type = nc.cloudnet_file_type
    nc.close()
    return file_type


def _is_date_in_range(path):
    date_in_file = int(path.name[:8])
    start = int(ARGS.start.replace('-', ''))
    stop = int(ARGS.stop.replace('-', ''))
    return start <= date_in_file < stop


def _get_fields_for_plot(cloudnet_file_type):
    max_alt = 10
    if cloudnet_file_type == 'categorize':
        fields = ['v']
    elif cloudnet_file_type == 'classification':
        fields = ['target_classification']
    elif cloudnet_file_type == 'iwc':
        fields = ['iwc']
    elif cloudnet_file_type == 'lwc':
        fields = ['lwc']
        max_alt = 6
    else:
        raise NotImplementedError
    return fields, max_alt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Plot quickloks from Cloudnet data.')
    parser.add_argument('site', nargs='+', help='Site Name', choices=['bucharest'])
    parser.add_argument('--start', type=str, metavar='YYYY-MM-DD', help='Starting date. Default is current day - 7.',
                        default=process_utils.get_date_from_past(7))
    parser.add_argument('--stop', type=str, metavar='YYYY-MM-DD', help='Stopping date. Default is current day - 1.',
                        default=process_utils.get_date_from_past(1))
    parser.add_argument('--output', type=str, metavar='/FOO/BAR', help='Output folder path. '
                                                                       'Overrides config/main.ini value.')
    parser.add_argument('-o', '--overwrite', dest='overwrite', action='store_true',
                        help='Overwrites existing images', default=False)
    ARGS = parser.parse_args()
    main()
