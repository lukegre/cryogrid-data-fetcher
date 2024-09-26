import numpy as np
import xarray as xr
from pystac import Item as STACItem
from memoization import cached as _cached

from .. import logger

@_cached
def get_land_cover_data(target_grid: xr.DataArray)->xr.DataArray:
    """
    Get the ESA World Cover dataset on the target grid and resolution

    The function searches the ESA World Cover dataset on Planetary Computer
    and downloads the data on the target grid and resolution. The data is
    reprojected to the target grid and returned as a DataArray. The function
    also adds the class names, descriptions, and colors as attributes to the
    DataArray for plotting.

    Parameters
    ----------
    target_grid : xr.DataArray
        The target grid to reproject the land cover data to. The bounding box
        and resolution of the target grid are used to search define the area 
        of interest and resolution of the data.

    Returns
    -------
    xr.DataArray
        A DataArray with the land cover data on the target grid. Contains 
        attributes 'class_values', 'class_descriptions', 'class_colors' for plotting.
    """
    from ..utils.stac_helpers import search_stac_items
    from ..utils.shp_helper import bbox_to_geopandas
    import stackstac

    logger.info("ESA World Cover: Fetching ESA World Cover dataset - see https://planetarycomputer.microsoft.com/dataset/esa-worldcover")
    logger.log("VERBOSE", "ESA World Cover: Product Version = 2.0.0 (DOI: https://zenodo.org/records/7254221)")
    crs_target = target_grid.rio.crs
    res_target = max(np.abs(target_grid.rio.resolution()))

    bbox_target = bbox_to_geopandas(target_grid.rio.bounds(), crs=crs_target)
    bbox_wsen_epsg4326 = bbox_target.to_crs('EPSG:4326')

    items = search_stac_items(
        collection='esa-worldcover', 
        bbox=bbox_wsen_epsg4326.bounds.values[0], 
        query={'esa_worldcover:product_version': {'eq': '2.0.0'}})
    
    da = stackstac.stack(
        items=items, 
        assets=['map'],
        epsg=crs_target.to_epsg(),
        bounds=bbox_target.total_bounds.tolist(),
        resolution=res_target)
    
    # removing the single band dimension
    da = da.max(['band', 'time'], keep_attrs=True)
    da = da.rio.reproject_match(target_grid).rename('land_cover')

    # getting the land cover class names and colors and adding as coords
    df = get_land_cover_classes(items[0])
    da.attrs['class_values'] = df.index.values
    da.attrs['class_descriptions'] = df['description'].values
    da.attrs['class_colors'] = df['color-hint'].values
    
    return da


def get_land_cover_classes(item: STACItem):
    """ 
    Get the land cover class names, and colors from the ESA World Cover dataset

    Parameters
    ----------
    item : pystac.Item
        A STAC item from the ESA World Cover dataset
    
    Returns
    -------
    pd.DataFrame
        A DataFrame with the class names, descriptions, and `color-hint`
    """
    import pandas as pd

    classes = item.assets['map'].extra_fields['classification:classes']
    df = pd.DataFrame(classes).set_index('value')

    df['color-hint'] = '#' + df['color-hint']

    return df

