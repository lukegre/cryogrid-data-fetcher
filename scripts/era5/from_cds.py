"""
Helper functions for downloading ERA5 data from the CDS API

Written to be used with the yaml_config_loader.py script
Downloads one file per year for single-level and pressure-level data

"""
import joblib
import pathlib
import fsspec
import dotenv
import cdsapi
from loguru import logger


def download_era5_from_cds(request_config: dict, dry_run=False):

    requests = make_cds_requests_list(request_config)
    
    n_jobs = request_config['era5']['n_parallel_requests']
    
    kwargs = dict(
        s3_dst_dir=request_config['era5']['dst_dir_s3'],
        remove_local=request_config['era5']['remove_local_files_after_s3_upload'],
        dry_run=dry_run)
    
    func = joblib.delayed(run_cds_request_copy_file_s3)
    tasks = [func(req, **kwargs) for req in requests]
    workers = joblib.Parallel(n_jobs=n_jobs, verbose=True)

    flist_s3 = workers(tasks)

    return flist_s3


def run_cds_request_copy_file_s3(request:list, s3_dst_dir:str, remove_local:bool=False, dry_run=False) -> str:
    """
    Run a CDS API request and copy the resulting file to an S3 bucket

    Parameters
    ----------
    request : list
        A list of the form [dataset, request_dict, output_name]
    s3_dst_dir : str
        The destination directory on S3
    remove_local : bool, optional
        Whether to remove the local file after copying to S3
    dry_run : bool, optional
        Whether to run the function without actually downloading or copying files
        only shows debug messages
    
    Returns
    -------
    str
        The path to the file on S3 after copying
    """
    dotenv.find_dotenv(raise_error_if_not_found=True)
    dotenv.load_dotenv(verbose=True)
    fs = fsspec.filesystem("s3")

    s3_dst_dir = s3_dst_dir.strip("/")

    fpath_local = pathlib.Path(request[-1])
    fpath_s3 = f"{s3_dst_dir}/{fpath_local.name}"

    exists_local = fpath_local.exists()
    exists_s3 = fs.exists(fpath_s3)

    if not exists_s3 and not exists_local:
        logger.debug(f"{fpath_local.name} does not exist locally nor on s3")
        if not dry_run:
            client = start_cdsapi_client()
            client.retrieve(*request)
            fs.put_file(fpath_local, fpath_s3)

    elif not exists_s3 and exists_local:
        logger.debug(f"{fpath_local.name} exists locally but not on s3")
        if not dry_run:
            fs.put_file(fpath_local, fpath_s3)

    elif exists_s3 and not exists_local:
        logger.debug(f"{fpath_local.name} exists on s3 but not locally")
        
    elif exists_s3 and exists_local:
        logger.debug(f"{fpath_local.name} exists on s3 and locally")
    
    if remove_local or not dry_run:
        assert fs.exists(fpath_s3), f"{fpath_s3} does not exist on s3"
        fpath_local.unlink()
    
    return fpath_s3


def start_cdsapi_client():

    # copy cdsapi from /secrets/cdsapi to ~/.cdsapirc
    try:
        import shutil
        import pathlib
        shutil.copy("/secrets/cdsapi", pathlib.Path("~/.cdsapirc").expanduser())
    except:
        pass

    c = cdsapi.Client()

    return c


def make_cds_requests_list(request_config: dict)->list:
    """
    Make a list of requests for the CDS API

    Parameters
    ----------
    bbox : list
        A list of floats representing the bounding box in the order [W, S, E, N]
    years : list
        A list of integers representing the years for which to download data

    Returns
    -------
    list
        A list of requests for the CDS API
    """
    from datetime import datetime
    req = request_config

    bbox = req['bbox_WSEN']
    years = range(req['start_year'], req['end_year'] + 1)
    output_fname_fmt = req['era5']['fname_local']

    single_levels = req['era5']['single_levels']
    pressure_levels = req['era5']['pressure_levels']

    requests = []
    for year in years:
        for month in range(1, 13):
            for day in range(1, 32):
                try:
                    # if this fails, then it's not a valid date and we skip the iteration
                    datetime(year, month, day)
                except ValueError:
                    continue
                requests += make_cds_request(bbox, year, output_fname_fmt, month=month, day=day, **single_levels),
                requests += make_cds_request(bbox, year, output_fname_fmt, month=month, day=day, **pressure_levels),
    
    return requests


def make_cds_request(bbox_WSEN:list, year:int, output_name_fmt:str, **kwargs)->dict:
    """
    Prepare a request for the CDS API that can be passed with
    client.retrieve(*request) 

    Parameters
    ----------
    bbox : list
        A list of floats representing the bounding box in the order [W, S, E, N]
    year : int
        The year for which to download data
    output_name_fmt : str
        A format string for the output file name
    kwargs : dict
        Additional keyword arguments to add to the request_dict. 
        must contain at least ['dataset', 'variable'] keys

    Returns
    -------
    list
        A list of the form [dataset, request_dict, output_name]
    """
    
    assert 'dataset' in kwargs, "Missing 'dataset' key in kwargs"
    assert 'variable' in kwargs, "Missing 'variable' key in kwargs"
    if 'pressure' in kwargs['dataset']:
        assert 'pressure_level' in kwargs, "Missing 'pressure_level' key in kwargs"

    dataset = kwargs.pop('dataset')
    name = dataset.replace("reanalysis-era5-", "").replace("-", "_")  # used for output file name (don't remove)
    request_dict = make_geospatial_request_defaults(bbox_WSEN, year, **kwargs)

    namespace = locals()
    namespace.update(request_dict)
    output_name = make_name_from_request(output_name_fmt, **namespace)
    
    request_api = [
        dataset, 
        request_dict,
        output_name,
    ]

    return request_api


def make_name_from_request(fname: str, **kwargs)->str:
    import copy

    namespace = copy.deepcopy(kwargs)

    for key in namespace:
        val = namespace[key]
        if isinstance(val, list) and len(val) == 1:
            namespace[key] = val[0]
    
    name = fname.format(**namespace)

    return name


def make_geospatial_request_defaults(bbox, year, month=range(1, 13), day=range(1, 32), hours=range(0, 24, 3), **kwargs):
    """
    Prepare a geospatial request for the CDS API

    The request is designed to download data for a specific year and month
    at a 3-hourly interval for the entire month within a bounding box.

    Parameters
    ----------
    bbox : list
        A list of floats representing the bounding box in the order [W, S, E, N]
    year : int
        The year for which to download data
    month : list, int, str, optional [1, 2, ..., 12]
        The month for which to download data
    days : list, optional [1, 2, ..., 31]
        A list of integers representing the days of the month for which to download data
    hours : list, optional [0, 3, 6, ..., 21]
        A list of integers representing the hours of the day for which to download
    kwargs : dict
        Additional keyword arguments to add to the request

    Returns
    -------
    dict
        A dictionary containing the geospatial request with the parameters
        year (list), month (list), day (list), time (list), area (list)
    """

    area_NWSE = [bbox[3], bbox[0], bbox[1], bbox[2]]

    if not isinstance(year, (int, str)):
        raise TypeError("`year` must be types [int|str]")
    
    years = [f"{int(year):04d}"]
    months = [f"{int(month):02d}"] if isinstance(month, (int, str)) else [f"{m:02d}" for m in month]
    days = [f"{int(day):02d}"] if isinstance(day, (int, str)) else [f"{d:02d}" for d in day]
    times = [f"{h:02d}:00" for h in hours]

    request = dict(
        product_type=['reanalysis'],
        data_format='netcdf',
        download_format='unarchived',
        year=years,
        month=months,
        day=days,
        time=times,
        area=area_NWSE)
    
    request.update(kwargs)
    
    return request
