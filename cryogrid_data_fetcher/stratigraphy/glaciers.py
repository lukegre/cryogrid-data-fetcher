# Description: Fetches glacier data from the Randolph Glacier Inventory

import geopandas as gpd
import xarray as xr
from .. import logger
from memoization import cached as _cached


URL_RGI_CENTRAL_ASIA = 'https://daacdata.apps.nsidc.org/pub/DATASETS/nsidc0770_rgi_v7/regional_files/RGI2000-v7.0-G/RGI2000-v7.0-G-13_central_asia.zip'


def download_url(url: str, target_dir=None):
    import pooch
    import earthaccess

    auth = earthaccess.login(persist=True)
    session = auth.get_session()

    flist = pooch.retrieve(
        url=url,
        known_hash=None,
        fname=url.split('/')[-1],
        path=target_dir,
        downloader=pooch.HTTPDownloader(
            progressbar=True, 
            headers=session.headers),
        processor=pooch.Unzip())
    
    return flist


@_cached
def read_randolph_glacier_inventory(url=URL_RGI_CENTRAL_ASIA, target_dem=None):
    import geopandas as gpd
    from ..utils.shp_helper import read_shapefile_and_clip_to_grid

    flist = download_url(url)
    fname_shp = [f for f in flist if f.endswith('.shp')][0]

    logger.log("INFO", f"RGI: Fetching Randolph Glacier Inventory - see https://www.glims.org/rgi_user_guide/welcome.html")
    logger.log("VERBOSE", f"RGI: URL = {URL_RGI_CENTRAL_ASIA}")
    logger.log("VERBOSE", f"RGI: FILE = {fname_shp}")
    if target_dem is not None:
        df = read_shapefile_and_clip_to_grid(fname_shp, target_dem)
    else:
        df = gpd.read_file(fname_shp)

    return df
