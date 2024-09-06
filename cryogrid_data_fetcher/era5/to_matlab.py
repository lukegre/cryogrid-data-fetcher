import numpy as np
import xarray as xr
from .. import logger
import pathlib


def main(
        s3_fnames:str, 
        matfile_fname:str, 
        bbox_WSEN:list, 
        year_start:int=None, 
        year_end:int=None, 
        local_cache_dir='./data/cache/'
    ):
    """
    Download ERA5 data from S3, clip to the region of interest, and save to MATLAB file.

    Parameters
    ----------
    s3_fnames : str or list
        List of S3 filenames or a string with wildcard characters 
        or an s3 folder. 
    matfile_fname : str
        Full path to the output MATLAB file.
    bbox_WSEN : list
        List of 4 floats: [West, South, East, North]
    year_start : int, optional
        Start year, by default None to use full dataset
    year_end : int, optional 
        End year, by default None to use full dataset
    local_cache_dir : str, optional
        Local directory to cache the data, by default './data/cache/' 
        if set, then the S3 files will be downloaded to this directory
        and then loaded - usually faster. 

    Returns
    -------
    str
        Full path to the MATLAB file.
    """
    from scipy.io import savemat
    from ..utils.xr_helpers import S3io

    def check_year(year):
        if isinstance(year, (int, str)):
            year = str(year)
        elif year is None:
            year = None
        else:
            raise TypeError(f'year must be int or str or None, not {type(year)}')
        return year

    matfile_fname = pathlib.Path(matfile_fname)
    nc_fname = matfile_fname.with_suffix('.nc')
    
    if matfile_fname.exists():
        logger.info(f'File exists: {matfile_fname}')
        return matfile_fname
    elif matfile_fname.suffix != '.mat':
        raise ValueError(f'matfile_fname must have .mat extension, not {matfile_fname.suffix}')

    year_start = check_year(year_start)
    year_end = check_year(year_end)
    
    if nc_fname.exists():
        logger.info(f'File exists: {nc_fname}')
        ds_exp = xr.open_dataset(nc_fname)
    else:
        # Load ERA5 files as a single dataset 
        ds_raw = S3io.open_mfdataset(s3_fnames, local_cache=local_cache_dir)
        # Clip to the region of interest and select the years of interest
        ds_exp = (
            ds_raw
            .rio.clip_box(*bbox_WSEN, crs='EPSG:4326')
            .sel(time=slice(year_start, year_end))
            .mean('expver')
            .astype('float32')
            .compute())
        # Copy attributes from raw data
        for key in ds_exp.data_vars.keys():
            ds_exp[key].attrs = ds_raw[key].attrs
        # Save to netcdf for later use
        ds_exp.to_netcdf(nc_fname, encoding={k: dict(zlib=True) for k in ds_exp.data_vars})
    
    # Save to MATLAB file
    era5_dict = get_era5_ds_as_dict(ds_exp)
    savemat(matfile_fname, era5_dict, do_compression=True)
    logger.success(f'Saved ERA5 to {matfile_fname}')

    return matfile_fname


def get_era5_ds_as_dict(ds: xr.Dataset)->dict:
    """
    Converrt a merged netCDF file from the Copernicus CDS to 
    a dictionary that matches the expected format of 
    the CryoGrid.POST_PROC.read_mat_ERA class (in MATLAB)

    Note
    ----
    I've commented out the pressure level variables for now

    Parameters
    ----------
    ds : xr.Dataset
        Dataset from the ERA5 Copernicus CDS with variables required for 
        the CryoGrid.POST_PROC.read_mat_ERA class 
        single_levels   = [u10, v10, sp, d2m, t2m, ssrd, strd, tisr, tp]
        pressure_levels = [t, z, q, u, v]

    Returns
    -------
    dict
        Dictionary with the variables mapped to names that are expected by 
        CryoGrid.POST_PROC.read_mat_ERA
        
    """
    # transpose to lon x lat x time (original is time x lat x lon)
    ds = ds.transpose('longitude', 'latitude', 'level', 'time')

    era = dict()
    era['dims'] = 'lon x lat (x pressure_levels) x time'
    # while lat and lon have to be [coord x 1]
    era['lat'] = ds['latitude'].values[:, None]
    era['lon'] = ds['longitude'].values[:, None]
    # pressure levels have to be [1 x coord] - only when pressure_levels present
    era['p'] = ds['level'].values[None] * 100 
    # time for some reason has to be [1 x coord]
    era['t'] = get_datetime_to_matlab_datenum(ds.time)[None] 
    # geopotential height at surface
    era['Zs'] = ds.Zs.values / 9.81  # gravity m/s2

    # single_level variables
    # wind and pressure (no transformations)
    era['u10'] = ds['u10'].values
    era['v10'] = ds['v10'].values
    era['ps'] = ds['sp'].values
    # temperature variables (degK -> degC)
    era['Td2'] = ds['d2m'].values - 273.15
    era['T2'] = ds['t2m'].values - 273.15
    # radiation variables (/sec -> /hour)
    era['SW'] = ds['ssrd'].values / 3600
    era['LW'] = ds['strd'].values / 3600
    era['S_TOA'] = ds['tisr'].values / 3600
    # precipitation (m -> mm)
    era['P'] = ds['tp'].values * 1000

    # pressure levels
    era['T'] = ds['t'].values - 273.15  # K to C
    era['Z'] = ds['z'].values / 9.81  # gravity m/s2
    era['q'] = ds['q'].values
    era['u'] = ds['u'].values
    era['v'] = ds['v'].values

    # scaling factors
    era['wind_sf'] = 1e-2
    era['q_sf'] = 1e-6
    era['ps_sf'] = 1e2
    era['rad_sf'] = 1e-1
    era['T_sf'] = 1e-2
    era['P_sf'] = 1e-2

    # apply scaling factors (done in the original, so we do it here)
    # wind scaling
    era['u']     = (era['u']     / era['wind_sf']).astype(np.int16)
    era['v']     = (era['v']     / era['wind_sf']).astype(np.int16)
    era['u10']   = (era['u10']   / era['wind_sf']).astype(np.int16)
    era['v10']   = (era['v10']   / era['wind_sf']).astype(np.int16)
    # temperature scaling
    era['T']     = (era['T']     / era['T_sf']   ).astype(np.int16)
    era['Td2']   = (era['Td2']   / era['T_sf']   ).astype(np.int16)
    era['T2']    = (era['T2']    / era['T_sf']   ).astype(np.int16)
    # humidity scaling
    era['q']     = (era['q']     / era['q_sf']   ).astype(np.uint16)
    # pressure scaling
    era['ps']    = (era['ps']    / era['ps_sf']  ).astype(np.uint16)
    # radiation scaling
    era['SW']    = (era['SW']    / era['rad_sf'] ).astype(np.uint16)
    era['LW']    = (era['LW']    / era['rad_sf'] ).astype(np.uint16)
    era['S_TOA'] = (era['S_TOA'] / era['rad_sf'] ).astype(np.uint16)
    # precipitation scaling
    era['P']     = (era['P']     / era['P_sf']   ).astype(np.uint16)
    # no scaling for geoportential height
    era['Z']     = era['Z'].astype(np.int16)

    return {'era': era}


def get_matlab_datenum_offset(reference_datestr):
    """
    Returns the matlab datenum offset for a given reference date string
    """
    import pandas as pd
    
    # this is hard coded in matlab, which uses 0000-01-01 as the reference date
    # but this isn't a valid date in pandas, so we use -0001-12-31 instead
    matlab_t0 = pd.Timestamp('-0001-12-31')  
    reference_date = pd.Timestamp(reference_datestr)
    offset_days = (matlab_t0 - reference_date).days
    
    return offset_days
    

def get_datetime_to_matlab_datenum(time: xr.DataArray, reference_datestr:str="1970-01-01") -> np.ndarray:
    """
    Converts the time dimension of a xarray dataset to matlab datenum format

    Parameters
    ----------
    time_hrs : xr.DataArray
        Time from dataset, but only supports hour resolution and lower (days, months, etc)
    reference_datestr : str
        Reference date string in format 'YYYY-MM-DD'. In many cases this is 1970-01-01

    Returns
    -------
    np.ndarray
        Array of matlab datenum values
    """

    hours_since_ref = time.values.astype('datetime64[h]').astype(float)
    days_since_ref = hours_since_ref / 24

    matlab_offset = get_matlab_datenum_offset(reference_datestr)
    matlab_datenum = days_since_ref - matlab_offset

    return matlab_datenum
