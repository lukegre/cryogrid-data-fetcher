from loguru import logger
import warnings
import xarray as xr

warnings.filterwarnings("ignore", message='.*infer_datetime_format.*')
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=UserWarning)


def main(bbox_WSEN, fname=None, res=30, progressbar=False):
    import fsspec

    if progressbar:
        logger.register_dask_progressbar()
    else:
        logger.unregister_dask_progressbar()

    if (fname is not None) and fname.startswith('s3'):
        fs = fsspec.filesystem('s3')
    else:
        fs = fsspec.filesystem('file')
    
    if (fname is not None) and fs.exists(fname):
        logger.info(f"File already exists: {fname}")
        return xr.open_zarr(fname)
    
    else:
        ds_doy = calc_sentinel2_snow_melt_day(bbox_WSEN, res=res)
        ds_mon = ds_doy.rename(snow_melt_doy='snow_melt_mon') / 30.5  # avg days in month
        ds_mon.snow_melt_mon.attrs = ds_doy.snow_melt_doy.attrs
        ds_mon.snow_melt_mon.attrs.update({'long_name': 'Month of snow melt', 'units': 'month'})
        if fname is not None:
            ds_mon.to_zarr(fname)
            logger.success(f"Saved to {fname}")
        return ds_mon


def calc_sentinel2_snow_melt_day(bbox: list, years=range(2018, 2025), res=30)->xr.DataArray:
    from ..utils.xr_helpers import coord_0d_to_attrs
    from pqdm.threads import pqdm
    from functools import partial

    n_jobs = len(years)
    func = partial(calc_sentinel2_snow_melt_day_for_single_year, bbox=bbox, res=res)
    da_list = pqdm(years, func, n_jobs=n_jobs, exception_behaviour='immediate')
    
    da = xr.concat(da_list, dim='year', coords='minimal', compat='override')
    da = coord_0d_to_attrs(da).assign_attrs({'long_name': 'Day of year of snow melt', 'units': 'day of year'})
    ds = da.to_dataset(name='snow_melt_doy').rio.write_crs(da.epsg).astype('float32')

    return ds


def calc_sentinel2_snow_melt_day_for_single_year(year: int, bbox: list, res=30)-> xr.DataArray:

    scl_snow_ice = 11   
    scl = get_sentinel2_scene_classification(bbox, year=year, res=res).compute()
    
    # find the last time step with good coverage and drop everything after
    # so that we can back fill the snow cover later
    scl_tail_clipped = drop_poor_coverage_at_end(scl, threshold=0.9)
    # mask snow/ice pixels and set values to 1 instead of 11
    snow_mask = scl_tail_clipped.where(lambda x: x == scl_snow_ice) * 0 + 1

    # find the time step where snow cover is the lowest, and remove anything after
    snow_melt = get_only_melt_period(snow_mask)
    # backfill the snow cover (assuming only melt) and create mask
    snow_mask = snow_melt.bfill('time').notnull()
    # compute the melt date based on a mask
    snow_melt_day = get_max_day_of_year_from_mask(snow_mask)
    
    snow_melt_day = snow_melt_day.expand_dims(year=[year])
    logger.success(f"Caculated snow melt day for {year}")

    return snow_melt_day


def get_sentinel2_scene_classification(bbox, year, epsg=32643, max_cloud_cover=30, res=100):
    from ..utils.stac_helpers import get_sentinel2_granules

    logger.info(f"Getting Sentinel-2 SCL granules @{res}m for {year} with max cloud cover = {max_cloud_cover}%")
    
    t0, t1 = f"{year}-01-01", f"{year}-11-15"  # assuming that snow melt is done by mid-November
    da_granules = get_sentinel2_granules(bbox, t0, t1, assets=['SCL'], max_cloud_cover=max_cloud_cover, epsg=epsg, res=res)
    
    da_timesteps = (
        da_granules  # granules are not grouped by time
        .groupby('time').max()  # take max value to avoid mixing ints
        .squeeze()  # remove the band dimension
        .where(lambda x: x > 0))  # mask null_values so that pixel coverage can be counted

    return da_timesteps


def get_max_day_of_year_from_mask(mask):
    assert 'time' in mask.dims, "'time' dimension is required"

    years = set(mask.time.dt.year.values.tolist())
    assert len(years) == 1, "Only one year is supported"

    doy_max = (
        mask.time.dt.dayofyear  # get the day of year
        .where(mask)  # broadcast the day of year to the mask shape
        .max('time')  # get the last time step 
        .astype('float32')
        .rename('day_of_year'))
    
    return doy_max


def drop_poor_coverage_at_end(da, threshold=0.9):
    """
    Drops the time steps at the end of the time series that 
    occur after the last point that meets the threshold req. 

    Example
    -------
    [0.4, 0.5, 0.3, 0.7, 0.9, 0.3]
    [keep keep keep keep keep drop]
    """
    counts = da.count(['x', 'y']).compute()
    size = da.isel(time=0).size
    frac = counts / size
    good_cover = (
        frac
        .bfill('time')
        .where(lambda x: x > threshold)
        .dropna('time')
        .time.values)
    return da.sel(time=slice(None, good_cover[-1]))


def find_time_of_lowest_snow_cover(snow_mask, window=10):
    """
    Returns the time step where the snow cover is the lowest
    """
    filled = snow_mask.rolling(time=window, center=True, min_periods=1).max()
    lowest_cover_time = filled.count(['x', 'y']).idxmin()
    return lowest_cover_time


def get_only_melt_period(snow_mask):
    """
    Drops time steps after snow starts increasing again
    """
    time_snow_cover_min = find_time_of_lowest_snow_cover(snow_mask)
    snow_melt_period = snow_mask.sel(time=slice(None, time_snow_cover_min))
    return snow_melt_period


def find_local_outlier_from_global_std(da, std_multiplier=2, **rolling_window):
    rolling = da.rolling(**rolling_window, center=True, min_periods=1)
    avg = rolling.mean()
    std = (da - avg).std() * std_multiplier
    thresh = avg + std
    outliers = (da > thresh)
    return outliers
