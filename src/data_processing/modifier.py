import netCDF4
from cloudnetpy.utils import get_uuid, get_time

MAJOR = 0
MINOR = 2
PATCH = 0
VERSION = '%d.%d.%d' % (MAJOR, MINOR, PATCH)


def fix_mwr_file(full_path: str,
                 original_filename: str,
                 date_str: str,
                 site_name: str):
    """Fixes global attributes of raw MWR netCDF file."""

    def _get_date():
        year = f'20{original_filename[:2]}'
        month = f'{original_filename[2:4]}'
        day = f'{original_filename[4:6]}'
        assert f'{year}-{month}-{day}' == date_str
        return year, month, day

    nc = netCDF4.Dataset(full_path, 'a')
    uuid = get_uuid()
    nc.file_uuid = uuid
    nc.cloudnet_file_type = 'mwr'
    nc.history = _add_history(nc)
    nc.year, nc.month, nc.day = _get_date()
    nc.location = site_name
    nc.title = _get_title(nc)
    if not hasattr(nc, 'Conventions'):
        nc.Conventions = 'CF-1.0'
    nc.close()
    return uuid


def fix_model_file(file_name):
    """Fixes global attributes of raw model netCDF file."""

    def _get_date():
        date_string = nc.variables['time'].units
        the_date = date_string.split()[2]
        return the_date.split('-')

    nc = netCDF4.Dataset(file_name, 'a')
    uuid = get_uuid()
    nc.file_uuid = uuid
    nc.cloudnet_file_type = 'model'
    nc.year, nc.month, nc.day = _get_date()
    nc.history = _add_history(nc)
    nc.title = _get_title(nc)
    nc.close()
    return uuid


def _add_history(nc):
    old_history = getattr(nc, 'history', '')
    new_record = f"{get_time()} - global attributes fixed using attribute_modifier {VERSION}\n"
    return f"{new_record}{old_history}"


def _get_title(nc):
    return f"{nc.cloudnet_file_type.capitalize()} file from {nc.location}"
