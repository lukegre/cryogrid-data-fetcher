
import xarray as _xr

from .. import logger


def main(config: dict)->_xr.Dataset:
    """
    Get the DEM data from the STAC catalog using the configuration file.

    The function checks if the DEM data is already downloaded to the given S3
    path. If not, the function downloads the DEM data from the STAC catalog
    and saves it to the S3 path. The function returns the DEM data as an
    xarray Dataset.

    Parameters
    ----------
    config : dict
        The configuration dictionary containing the DEM data information. 
        See cdf.config.make_template for an example.

    Returns
    -------
    xarray.Dataset
        The DEM data as an xarray Dataset - variable name 'elevation'.
    """
    import fsspec
    
    fs = fsspec.filesystem('s3')

    if not fs.exists(config.dem.fpath_s3):
        dem = get_dem_from_stac_with_config(config)
        dem.s3.to_zarr(config.dem.fpath_s3)
    else:
        logger.info(f"Loading DEM from {config.dem.fpath_s3}")
    
    ds = _xr.open_zarr(config.dem.fpath_s3)
    ds = ds.rio.write_crs(config.dem.epsg)
    
    return ds


def download_dem_data(bbox_WSEN:list, res_m:int=30, collection:str='cop-dem-glo-30', epsg=32643)->_xr.DataArray:
    """
    Download DEM data from the STAC catalog (default is COP DEM Global 30m).

    The function searches the STAC catalog for the given collection and
    downloads the data for the given bounding box and resolution. The function
    returns the DEM data as an xarray DataArray.

    Parameters
    ----------
    bbox_WSEN : list
        The bounding box of the area of interest in WSEN format.
    res_m : int
        The resolution of the DEM data in meters.
    collection : str
        The name of the STAC collection to search for the DEM data.
    epsg : int (optional)
        The EPSG code of the projection of the DEM data. Default is 
        EPSG:32643 (UTM 43N) for the Pamir region. 

    Returns
    -------
    xarray.DataArray
        The DEM data as an xarray DataArray with attributes.
    """
    from ..utils.xr_helpers import coord_0d_to_attrs
    from ..utils.stac_helpers import search_stac_items
    import stackstac


    if epsg == 4326:
        res = res_m / 111111
    else:
        res = res_m

    items = search_stac_items(collection, bbox_WSEN)
    da_dem = stackstac.stack(
        items=items, 
        bounds_latlon=bbox_WSEN,
        resolution=res,
        epsg=epsg)
    
    da_dem = (
        da_dem
        .mean('time')
        .squeeze()
        .pipe(coord_0d_to_attrs)
        .assign_attrs(source=collection)
    )

    epsg = da_dem.attrs['epsg']
    da_dem = da_dem.rio.write_crs(epsg).rename('elevation')

    return da_dem


def get_dem_from_stac_using_config(config: dict)->_xr.Dataset:
    """
    A wrapper around download_dem_data to get the DEM data from the STAC catalog
    using the configuration dictionary.

    Parameters
    ----------
    config : dict
        The configuration dictionary containing the DEM data information. 
        See cdf.config.make_template for an example.
    
    Returns
    -------
    xarray.Dataset
        The DEM data as an xarray Dataset - variable name 'elevation'.
    """
    bbox = config['bbox_WSEN']
    stac_url = config['dem']['stac_catalog_url']
    stac_col = config['dem']['stac_collection']
    epsg = config['dem']['epsg']
    res = config['dem']['resolution']
    
    logger.info(f"Getting DEM data from {stac_url}/{stac_col} for WSEN: {bbox}")
    da_dem = download_dem_data(bbox, res, stac_col, epsg)
    ds_dem = da_dem.to_dataset(name='elevation')
    
    return ds_dem
