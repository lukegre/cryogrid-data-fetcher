import fsspec
import xarray as xr
import pandas as pd
from loguru import logger
from ..utils import xr_helpers as xh


def download_era5_from_weatherbench(data_config, download_batch_size='1MS'):

    time_start = pd.Timestamp(str(data_config['start_year']))
    time_end = pd.Timestamp(year=int(data_config['end_year']) + 1, month=1, day=1)
    dates = pd.date_range(time_start, time_end, freq=download_batch_size)

    file_list = [get_geopotential(data_config)]
    for t0, t1 in zip(dates[:-1], dates[1:]):
        file_list += download_era5_weatherbench_batch(data_config, t0, t1),

    return file_list


def make_s3_path(data_config: dict, t0: pd.Timestamp, t1: pd.Timestamp)->str:
    
    # year and month are used in the name of the file
    # don't delete these lines (used in format strings)
    t0_year = f"{t0.year:04d}"
    t0_month = f"{t0.month:02d}"
    t0_day = f"{t0.day:02d}"
    t1_year = f"{t1.year:04d}"
    t1_month = f"{t1.month:02d}"
    t1_day = f"{t1.day:02d}"

    sname = data_config['era5']['fname'].format(**locals())
    s3_path = f"{data_config['era5']['dst_dir_s3']}/{sname}"
    xh.is_safe_s3_path(s3_path)

    return s3_path
        

def download_era5_weatherbench_batch(data_config:dict, t0:pd.Timestamp, t1:pd.Timestamp)->str:
    from pqdm.threads import pqdm
    
    s3_path = make_s3_path(data_config, t0, t1)

    fs = fsspec.filesystem('s3')

    if fs.exists(s3_path):
        logger.info(f"File already exists: {s3_path}")
        return s3_path

    logger.info(f"Downloading ERA5 data: {t0:%Y-%m-%d} to {t1:%Y-%m-%d}")
    
    urls = make_weatherbench_era5_url_list(data_config, t0, t1)
    subset_netcdf = make_gc_netcdf_subsetter(data_config)
    n_jobs = data_config['era5']['n_jobs']
    
    results = pqdm(urls, subset_netcdf, n_jobs=n_jobs, exception_behaviour='immediate')
    ds = xr.merge(results)

    ds.s3.to_netcdf(s3_path)
    return s3_path
    
    
def make_weatherbench_era5_url_list(data_config:dict, t0:str, t1:str)->list:
    import pandas as pd
    
    dates = pd.date_range(t0, t1, freq='1D', inclusive='left')
    url_list = []
    for var in data_config['era5']['pressure_levels']['variable']:
        for level in data_config['era5']['pressure_levels']['pressure_level']:
            for t in dates:
                url = make_weatherbench_era5_url(t=t, variable=var, level=level)
                url_list.append(url)

    for var in data_config['era5']['single_levels']['variable']:
        for t in dates:
            url = make_weatherbench_era5_url(t=t, variable=var)
            url_list.append(url)

    return url_list


def make_weatherbench_era5_url(**kwargs)->str:
    url_pres = "gs://gcp-public-data-arco-era5/raw/date-variable-pressure_level/{t:%Y}/{t:%m}/{t:%d}/{variable}/{level}.nc"
    url_surf = "gs://gcp-public-data-arco-era5/raw/date-variable-single_level/{t:%Y}/{t:%m}/{t:%d}/{variable}/surface.nc"

    if 'level' in kwargs:
        return url_pres.format(**kwargs)
    else:
        return url_surf.format(**kwargs)
    

def make_gc_netcdf_subsetter(data_config)->callable:
    import pathlib
    import xarray as xr
    import rioxarray  # imported to have rio accessors

    fs = fsspec.filesystem('gs')
    bbox = data_config['bbox_WSEN']

    def subset_gc_netcdf(url):

        file = fs.open(url)
        name = pathlib.Path(url).stem

        ds = (
            xr.open_dataset(file, chunks={})
            .isel(time=slice(0, None, 3))
            .rio.write_crs(4326)
            .rio.clip_box(*bbox, crs='EPSG:4326')
            .compute())

        if name != 'surface':
            ds = ds.expand_dims(level=[int(name)])

        return ds
    
    return subset_gc_netcdf
    

def get_geopotential(config:dict)->str:
    """
    Downloads the ERA5 geopotential data from weatherbench and saves it to s3 
    Only needs to be done once for the entire request
    """
    import re
    from ..utils.s3_helpers import is_safe_s3_path

    fname = re.sub(r'\{.*\}', 'geopotential', config.era5.fname)
    s3_path = f"{config.era5.dst_dir_s3}/{fname}"
    
    is_safe_s3_path(s3_path)

    fs = fsspec.filesystem('s3')
    if fs.exists(s3_path):
        logger.info(f"File already exists: {s3_path}")
    else:
        logger.info("Downloading ERA5 geopotential data")
        # create the function that subsets to the specific region and downloads the data
        func_subset_era5 = make_gc_netcdf_subsetter(config)
        # make the url to download - surface geopotential at any time - here we use 2000
        url = make_weatherbench_era5_url(t=pd.Timestamp('2000'), variable='geopotential')
        
        # process the data 
        ds = func_subset_era5(url).mean('time').rename(z='Zs')

        ds.s3.to_netcdf(s3_path)
    
    return s3_path
