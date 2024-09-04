import numpy as np
import xarray as xr


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
    ds = ds.transpose('longitude', 'latitude', 'time')

    era = dict()
    era['dims'] = 'lon x lat (x pressure_levels) x time'
    # while lat and lon have to be [coord x 1]
    era['lat'] = ds['latitude'].values[:, None]
    era['lon'] = ds['longitude'].values[:, None]
    # pressure levels have to be [1 x coord] - only when pressure_levels present
    era['p'] = ds['level'].values[None] * 100 
    # time for some reason has to be [1 x coord]
    era['t'] = get_datetime_to_matlab_datenum(ds.time)[None] 

    # single_level variables
    # wind and pressure (no transformations)
    era['u10'] = ds['u10'].values
    era['v10'] = ds['v10'].values
    era['ps'] = ds['sp'].values
    # temperature variables (degK -> degC)
    era['T2d'] = ds['d2m'].values - 273.15
    era['T2'] = ds['t2m'].values - 273.15
    # radiation variables (/sec -> /hour)
    era['SW'] = ds['ssrd'].values / 3600
    era['LW'] = ds['strd'].values / 3600
    era['S_TOA'] = ds['tisr'].values / 3600
    # precipitation (m -> mm)
    era['P'] = ds['tp'].values * 1000

    # pressure levels
    era['T'] = ds['t'] - 273.15  # K to C
    era['Z'] = ds['z'] / 9.81  # gravity m/s2
    era['q'] = ds['q']
    era['u'] = ds['u']
    era['v'] = ds['v']

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
    era['T2d']   = (era['T2d']   / era['T_sf']   ).astype(np.int16)
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

    return era