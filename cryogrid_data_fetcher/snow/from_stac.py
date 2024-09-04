from loguru import logger
import pystac_client
import planetary_computer
import stackstac
import warnings

warnings.filterwarnings("ignore", message='.*infer_datetime_format.*')


def main(bbox, epsg=32643, max_cloud_cover=30, res=100):
    import xarray as xr

    years = range(2018, 2024)
    smelt_day = []
    for year in years:
        logger.info(f'Getting snow melt day of year for {year}')
        ds = get_smelt_day(bbox, year, epsg, max_cloud_cover, res)
        smelt_day += ds.smelt_doy,
    
    smelt_day = xr.combine_nested(smelt_day, concat_dim='year', coords='minimal')
    smelt_day = smelt_day.assign_coords(year=years)
    
    smelt_day = remove_outliers(smelt_day)

    return smelt_day


def remove_outliers(da, multiplier=1.5):
    avg = da.mean('year')
    std = da.std('year')

    # remove outliers upper
    upper = avg + (multiplier * std)
    lower = avg - (multiplier * std)

    da = da.where((da > lower) & (da < upper))

    return da


def get_smelt_day(bbox, year, epsg=32643, max_cloud_cover=30, res=100):
    import xarray as xr

    t0, t1 = f"{year}-01-01", f"{year}-12-31"

    logger.info(f"Getting snow mask for {year}")
    scl = get_s2_scene_classification(bbox, t0, t1, max_cloud_cover, epsg, res)
    
    logger.info('Masking clouds, ffilling 5 days, removing bad cover (<80%)')
    scl_prep = prep_s2_scl_clouds(scl)
    snow_mask = (scl_prep == 11).where(scl_prep > 0)

    logger.info('Calculating day with minimum snow cover')
    t_snow_min = _get_t_snow_minimum(snow_mask).compute()
    t_snow_min_pandas = t_snow_min.expand_dims(time=1).to_index()[0]
    snow_mask = snow_mask.sel(time=slice(None, t_snow_min)).rename('snow_mask')
    
    logger.info(f'Calculating snow melt day between {t0} and {t_snow_min_pandas: %Y-%m-%d}')
    smelt_day = _get_smelt_day_from_snow_mask(snow_mask)

    da = xr.merge([snow_mask, smelt_day])
    
    return da


def get_s2_scene_classification(bbox, start_date, end_date, max_cloud_cover=30, epsg=32643, res=100):
    
    items = get_items(
        "sentinel-2-l2a",
        bbox=bbox,
        datetime=f"{start_date}/{end_date}",
        query={"eo:cloud_cover": {"lt": max_cloud_cover}})
    
    da = stackstac.stack(
        items, 
        assets=["SCL"], 
        epsg=epsg, 
        bounds_latlon=bbox,
        chunksize=2048,
        resolution=res)
    
    da = da.isel(band=0).rename("SCL")
    
    return da


def prep_s2_scl_clouds(scl):
    
    scl_cloud_free = _mask_clouds(scl)
    scl_filled = _ffill_cloud_gaps(scl_cloud_free)
    scl_filled_80pct_cover = _remove_poor_cover(scl_filled, 0.8)

    return scl_filled_80pct_cover


def _remove_poor_cover(da, cover_threshold=0.80):
    size = da.isel(time=0).size
    n_obs = (da > 0).sum(dim=['x', 'y'])
    pct_cover = n_obs / size
    good_cover = (pct_cover > cover_threshold).compute()

    da = da[good_cover]
    return da
    

def _get_smelt_day_from_snow_mask(snow_mask):
    doy = snow_mask.time.dt.dayofyear
    smelt_day = doy.where(snow_mask).max('time')
    return smelt_day.rename('smelt_doy')


def _get_t_snow_minimum(snow_mask):
    count = snow_mask.fillna(0).sum(dim=['x', 'y'])
    count_smooth = count.rolling(time=3, center=True).max()
    snow_minimum = count_smooth.idxmin(dim='time')
    return snow_minimum


def _mask_clouds(scl):
    mask = scl.isin([7, 8, 9, 10])
    scl_no_clouds = scl.where(~mask)
    return scl_no_clouds


def _ffill_cloud_gaps(scl_no_clouds, ffill_limit_days=5):
    da = scl_no_clouds.resample(time="1D").median()
    da = da.ffill("time", limit=ffill_limit_days)
    da = da.sel(time=scl_no_clouds.time, tolerance='1D', method='ffill')
    return da


def get_catalog():

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace)
    
    return catalog


def get_items(collection, catalog=None, **query):

    if catalog is None:
        catalog = get_catalog()
        
    search = catalog.search(collections=[collection], **query)
    items = list(search.items())

    logger.info(f"Returned {len(items)} items")

    return items