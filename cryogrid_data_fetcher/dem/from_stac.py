import fsspec
import xarray as _xr

from .. import logger


def main(config: dict)->_xr.Dataset:

    fs = fsspec.filesystem('s3')

    if not fs.exists(config.dem.fpath_s3):
        dem = get_stac_data(config)
        dem.s3.to_zarr(config.dem.fpath_s3)
    else:
        logger.info(f"Loading DEM from {config.dem.fpath_s3}")
    
    ds = _xr.open_zarr(config.dem.fpath_s3)
    ds = ds.rio.write_crs(config.dem.epsg)
    
    return ds


def download_dem_to_s3(config: dict)->None:
    
    dem = get_stac_data(config)
    
    fs = fsspec.filesystem('s3')
    mapper = fs.get_mapper(config.dem.fpath_s3)
    logger.info(f"Writing DEM data to {config.dem.fpath_s3}")

    dem.to_zarr(mapper, mode='w')

    logger.success(f"DEM data written to {config.dem.fpath_s3}")


def get_stac_data(config: dict)->_xr.Dataset:
    import stackstac
    from ..utils.xr_helpers import coord_0d_to_attrs
    from ..utils.stac_helpers import search_stac_items

    bbox = config['bbox_WSEN']
    stac_url = config['dem']['stac_catalog_url']
    stac_col = config['dem']['stac_collection']
    epsg = config['dem']['epsg']
    res = config['dem']['resolution']
    
    logger.info(f"Getting DEM data from {stac_url}/{stac_col} for WSEN: {bbox}")
    items = search_stac_items(stac_url, stac_col, bbox)
    da_dem = stackstac.stack(
        items=items, 
        bounds_latlon=bbox,
        resolution=res,
        epsg=epsg)
    
    ds_dem = (
        da_dem
        .mean('time')
        .to_dataset(name='elevation')
        .squeeze()
        .pipe(coord_0d_to_attrs)
        .assign_attrs(source=stac_col)
        .rio.write_crs(epsg)
    )
    
    return ds_dem
