"""
Installation of dependencies:
1. conda install gdal=3.9.2
2. pip install planetary_computer stackstac xarray pystac-client dask (and other requirements)

Usage:
1. from download_dem_stac import get_stac_data
2. Get the bounding box (bbox_WSEN) of the area of interest in WSEN format from http://bboxfinder.com/
3. dem = get_stac_data(bbox_WSEN, res_m=30, epsg=32643)  # for UTM 43N projection
4. dem.rio.to_raster('dem_30m.tif')
"""

import xarray as _xr
import planetary_computer
import pystac_client
import stackstac


URL_PLANETARY_COMPUTER = "https://planetarycomputer.microsoft.com/api/stac/v1"


def search_stac_items(collection, bbox, url=URL_PLANETARY_COMPUTER, **kwargs)->list:

    catalog = pystac_client.Client.open(
        url=url,
        modifier=planetary_computer.sign_inplace)

    search = catalog.search(
        collections=[collection],
        bbox=bbox, 
        **kwargs)

    items = search.item_collection()

    return items


def get_stac_data(bbox_WSEN:list, res_m:int=30, collection:str='cop-dem-glo-30', epsg=32643)->_xr.Dataset:
    
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
    da_dem = da_dem.rio.write_crs(epsg)

    return da_dem


def coord_0d_to_attrs(ds):
    attrs = {}
    for coord in ds.coords:
        if ds[coord].shape == ():
            val = ds[coord].values
            try:
                val = str(val).replace(':', '_')
                attrs[coord] = val
            except:
                pass
    ds.attrs.update(attrs)
    ds = ds.drop_vars(attrs.keys())
    return ds
