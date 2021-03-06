import datetime
import configparser
import hashlib
import requests
from typing import Tuple, Union
from cloudnetpy.utils import get_time
from cloudnetpy.plotting.plot_meta import ATTRIBUTES as ATTR
import base64
import netCDF4


def create_product_put_payload(full_path: str,
                               storage_service_response: dict,
                               product: str = None,
                               site: str = None,
                               date_str: str = None,
                               model: str = None) -> dict:
    nc = netCDF4.Dataset(full_path, 'r')

    payload = {
        'product': product or nc.cloudnet_file_type,
        'site': site or nc.location.lower(),
        'measurementDate': date_str or f'{nc.year}-{nc.month}-{nc.day}',
        'format': get_file_format(nc),
        'checksum': sha256sum(full_path),
        'volatile': not hasattr(nc, 'pid'),
        'uuid': getattr(nc, 'file_uuid', ''),
        'pid': getattr(nc, 'pid', ''),
        'history': getattr(nc, 'history', ''),
        'cloudnetpyVersion': getattr(nc, 'cloudnetpy_version', ''),
        ** storage_service_response
    }
    source_uuids = getattr(nc, 'source_file_uuids', None)
    if source_uuids:
        payload['sourceFileIds'] = source_uuids.replace(' ', '').split(',')
    if model:
        payload['model'] = model
    nc.close()
    return payload


def get_file_format(nc: netCDF4.Dataset):
    file_format = nc.file_format.lower()
    if 'netcdf4' in file_format:
        return 'HDF5 (NetCDF4)'
    if 'netcdf3' in file_format:
        return 'NetCDF3'
    raise RuntimeError('Unknown file type')


def read_site_info(site_name: str) -> dict:
    """Read site information from Cloudnet http API."""
    url = f"https://cloudnet.fmi.fi/api/sites?developer"
    sites = requests.get(url=url).json()
    for site in sites:
        if site['id'] == site_name:
            site['id'] = site_name
            site['name'] = site.pop('humanReadableName')
            return site


def get_product_types(level: int = None) -> list:
    """Return Cloudnet processing types."""
    url = f"https://cloudnet.fmi.fi/api/products"
    products = requests.get(url=url).json()
    l1_types = [product['id'] for product in products if int(product['level']) == 1]
    l2_types = [product['id'] for product in products if int(product['level']) == 2]
    l1_types.remove('categorize')
    if level == 1:
        return l1_types
    if level == 2:
        return l2_types
    return l1_types + ['categorize'] + l2_types


def get_model_types() -> list:
    url = f"https://cloudnet.fmi.fi/api/models"
    models = requests.get(url=url).json()
    return [model['id'] for model in models]


def date_string_to_date(date_string: str) -> datetime.date:
    """Convert YYYY-MM-DD to Python date."""
    date = [int(x) for x in date_string.split('-')]
    return datetime.date(*date)


def get_date_from_past(n: int, reference_date: str = None) -> str:
    """Return date N-days ago.

    Args:
        n (int): Number of days to skip (can be negative, when it means the future).
        reference_date (str, optional): Date as "YYYY-MM-DD". Default is the current date.

    Returns:
        str: Date as "YYYY-MM-DD".

    """
    reference = reference_date or get_time().split()[0]
    date = date_string_to_date(reference) - datetime.timedelta(n)
    return str(date)


def read_main_conf(args):
    """Read data-processing main config-file."""
    config_path = f'{args.config_dir}/main.ini'
    config = configparser.ConfigParser()
    config.read_file(open(config_path, 'r'))
    return config


def str2bool(s: str) -> Union[bool, str]:
    return False if s == 'False' else True if s == 'True' else s


def get_fields_for_plot(cloudnet_file_type: str) -> Tuple[list, int]:
    """Return list of variables and maximum altitude for Cloudnet quicklooks.

    Args:
        cloudnet_file_type (str): Name of Cloudnet file type, e.g., 'classification'.

    Returns:
        tuple: 2-element tuple containing feasible variables for plots
        (list) and maximum altitude (int).

    """
    max_alt = 10
    if cloudnet_file_type == 'categorize':
        fields = ['Z', 'v', 'width', 'ldr', 'v_sigma', 'beta', 'lwp', 'Tw',
                  'radar_gas_atten', 'radar_liquid_atten']
    elif cloudnet_file_type == 'classification':
        fields = ['target_classification', 'detection_status']
    elif cloudnet_file_type == 'iwc':
        fields = ['iwc', 'iwc_error', 'iwc_retrieval_status']
    elif cloudnet_file_type == 'lwc':
        fields = ['lwc', 'lwc_error', 'lwc_retrieval_status']
        max_alt = 6
    elif cloudnet_file_type == 'model':
        fields = ['cloud_fraction', 'uwind', 'vwind', 'temperature', 'q', 'pressure']
    elif cloudnet_file_type == 'lidar':
        fields = ['beta', 'beta_raw']
    elif cloudnet_file_type == 'mwr':
        fields = ['LWP']
    elif cloudnet_file_type == 'radar':
        fields = ['Ze', 'v', 'width', 'ldr']
    elif cloudnet_file_type == 'drizzle':
        fields = ['Do', 'mu', 'S', 'drizzle_N', 'drizzle_lwc', 'drizzle_lwf', 'v_drizzle', 'v_air']
        max_alt = 4
    else:
        raise NotImplementedError
    return fields, max_alt


def get_plottable_variables_info(cloudnet_file_type: str) -> dict:
    """Find variable IDs and corresponding human readable names."""
    fields = get_fields_for_plot(cloudnet_file_type)[0]
    return {get_var_id(cloudnet_file_type, field): [f"{ATTR[field].name}", i]
            for i, field in enumerate(fields)}


def get_var_id(cloudnet_file_type: str, field: str) -> str:
    """Return identifier for variable / Cloudnet file combination."""
    return f"{cloudnet_file_type}-{field}"


def sha256sum(filename: str) -> str:
    """Calculates hash of file using sha-256."""
    return _calc_hash_sum(filename, 'sha256')


def md5sum(filename: str, is_base64=False) -> str:
    """Calculates hash of file using md5."""
    return _calc_hash_sum(filename, 'md5', is_base64)


def _calc_hash_sum(filename, method, is_base64=False):
    hash_sum = getattr(hashlib, method)()
    with open(filename, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_sum.update(byte_block)
    if is_base64:
        return base64.encodebytes(hash_sum.digest()).decode('utf-8').strip()
    return hash_sum.hexdigest()


def get_product_bucket(volatile: bool = False) -> str:
    return 'cloudnet-product-volatile' if volatile else 'cloudnet-product'


def is_volatile_file(filename: str) -> bool:
    """Check if nc-file is volatile."""
    nc = netCDF4.Dataset(filename)
    is_missing_pid = not hasattr(nc, 'pid')
    nc.close()
    return is_missing_pid


def get_product_identifier(product: str) -> str:
    if product == 'iwc':
        return 'iwc-Z-T-method'
    if product == 'lwc':
        return 'lwc-scaled-adiabatic'
    return product


def get_model_identifier(filename: str) -> str:
    return filename.split('_')[-1][:-3]


class MiscError(Exception):
    """Internal exception class."""
    def __init__(self, msg: str):
        self.message = msg
        super().__init__(self.message)


class RawDataMissingError(Exception):
    """Internal exception class."""
    def __init__(self, msg: str):
        self.message = msg
        super().__init__(self.message)
