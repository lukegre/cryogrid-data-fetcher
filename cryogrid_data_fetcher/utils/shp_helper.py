from .. import logger
import geopandas as gpd
import xarray as xr

gpd.options.io_engine = "pyogrio"

_LEAFLET_DEFAULTS = dict()
GOOGLE_TERRAIN = dict(tiles='http://mt0.google.com/vt/lyrs=p&hl=en&x={x}&y={y}&z={z}', attr='Google', **_LEAFLET_DEFAULTS)
GOOGLE_SATELLITE = dict(tiles='http://mt0.google.com/vt/lyrs=s&hl=en&x={x}&y={y}&z={z}', attr='Google', **_LEAFLET_DEFAULTS)


def read_shapefile_and_clip_to_grid(fname_shapefile: str, target_grid: xr.DataArray)->gpd.GeoDataFrame:

    logger.log("DEBUG", f"Reading shape file: {fname_shapefile}")
    # 1. get crs of target grid
    crs_target = target_grid.rio.crs
    # 2. get bbox of target grid in target crs
    bbox_target = bbox_to_geopandas(target_grid.rio.bounds(), crs=crs_target)

    # 3. get crs of shape file (quick read)
    logger.log('DEBUG', "Reading single line in shape file to get CRS")
    crs_shapefile = gpd.read_file(fname_shapefile, rows=1).crs

    # 4. get bbox of shape file in target crs
    logger.log('DEBUG', f"Converting shape file bounding box to target CRS [EPSG:{crs_shapefile.to_epsg()}]")
    bbox_shapefile = bbox_target.to_crs(crs_shapefile)

    # 5. read shape file in native crs
    logger.log('DEBUG', f"Reading shape file and clipping to BBOX {bbox_shapefile.total_bounds}")
    df_native_crs = gpd.read_file(fname_shapefile, engine='pyogrio', bbox=bbox_shapefile)

    # 6. reproject shape file to target
    logger.log('DEBUG', f"Reprojecting shape file to target CRS [EPSG:{crs_target.to_epsg()}]")
    df = df_native_crs.to_crs(crs_target)

    # 7. clip shape file to target bbox
    logger.log('DEBUG', f"Clipping shape file to target BBOX {bbox_target.total_bounds}")
    df = df.clip(bbox_target, keep_geom_type=True)

    # 8. drop empty geometries after clipping
    df = df[~df.is_empty]

    return df


def clip_geodata_to_grid(df: gpd.GeoDataFrame, target_grid: xr.DataArray)->gpd.GeoDataFrame:
    
    # 1. get crs of target grid
    crs_target = target_grid.rio.crs
    # 2. get bbox of target grid in target crs
    bbox_target = bbox_to_geopandas(target_grid.rio.bounds(), crs=crs_target)

    # 3. get crs of shape file (quick read)
    crs_shapefile = df.crs

    # 5. reproject shape file to target
    df_target_crs = df.to_crs(crs_target)
    # 6. clip shape file to target bbox
    geometry_target_crs_clipped = df_target_crs.clip_by_rect(*bbox_target.total_bounds)
    df_target_crs['geometry'] = geometry_target_crs_clipped

    return df_target_crs


def bbox_to_geopandas(bbox: tuple, crs='EPSG:4326'):
    """
    Convert a bounding box to a GeoPandas DataFrame with a defined CRS.

    Parameters
    ----------
    bbox : tuple
        A tuple with the bounding box coordinates (west, south, east, north).
    crs : str, optional
        The CRS of the bounding box. The default is 'EPSG:4326'.

    Returns
    -------
    gpd.GeoDataFrame
        A GeoPandas DataFrame with a single row containing the bounding box as a polygon.
        To return a bbox, use df_bbox.total_bounds
    """
    import geopandas as gp
    from shapely.geometry import box

    logger.log("DEBUG", f"Converting bounding box to GeoPandas Series: {bbox}")
    bbox = box(*bbox)
    gdf = gp.GeoDataFrame(geometry=[bbox], crs=crs)

    return gdf


def to_kml(df, fname, **kwargs):
    """Just a wrapper, but helps to avoid Googling"""
    df.to_file(fname, driver='KML', **kwargs)


def polygon_to_raster_bool(polygon, da_target):
    """
    Convert a Shapely polygon to a binary mask that matches the grid of an xarray.DataArray.

    Parameters
    ----------
    polygon : shapely.geometry.Polygon or geopandas.GeoDataFrame
        The polygon to convert to a raster mask.
    da_target : xr.DataArray
        The target grid to match the mask to (spatial dimensions must be 'x' and 'y').
    
    Returns
    -------
    mask_da : xr.DataArray
        A boolean DataArray with the mask on the target grid.
    """
    import rasterio
    import numpy as np
    import xarray as xr
    from rasterio.features import rasterize
    from shapely.geometry import mapping

    # logger.debug('Converting polygon to raster mask')

    if isinstance(polygon, (gpd.GeoSeries, gpd.GeoDataFrame)):
        logger.debug('Converting GeoDataFrame to unary union')
        polygon = polygon.unary_union

    # Get the spatial dimensions of the data array
    if 'x' in da_target.dims and 'y' in da_target.dims:
        x, y = 'x', 'y'
    else:
        raise ValueError("Data array must have 'x' and 'y' dimensions")

    # Define the transformation from pixel coordinates to geographical coordinates
    transform = rasterio.transform.from_bounds(min(da_target[x].values), min(da_target[y].values),
                                               max(da_target[x].values), max(da_target[y].values),
                                               len(da_target[x]), len(da_target[y]))

    # Rasterize the polygon
    mask = rasterize([mapping(polygon)], out_shape=(len(da_target[y]), len(da_target[x])),
                     transform=transform, fill=0, out=None, all_touched=True, dtype=np.uint8)

    # Create a DataArray from the mask
    mask_da = xr.DataArray(mask, dims=(y, x), coords={y: da_target[y], x: da_target[x]}).astype(bool)

    return mask_da


def polygons_to_raster_int(df: gpd.GeoDataFrame, da_target: xr.DataArray, by_column=None, **joblib_kwargs)->xr.DataArray:
    """
    Convert a GeoDataFrame with polygons to a raster mask with integer values.

    Each row of polygons in the GeoDataFrame is converted to a separate integer 
    value in the raster mask. The integer values are assigned in the order of the
    rows in the GeoDataFrame. The conversion is run in parallel using joblib.

    Parameters
    ----------
    df : gpd.GeoDataFrame
        The GeoDataFrame with polygons to convert to a raster mask.
    da_target : xr.DataArray
        The target grid to match the mask to (spatial dimensions must be 'x' and 'y').
    by_column : str, optional
        The column in the GeoDataFrame to group the polygons by. If None, then each
        row is converted to a separate integer value. The default is None.
    joblib_kwargs : dict, optional
        Additional keyword arguments to pass to joblib.Parallel. The default is {}.

    Returns
    -------
    xr.DataArray
        A DataArray with the raster mask with integer values.
    """
    import joblib

    if by_column is not None:
        assert by_column in df.columns, f"Column {by_column} not found in DataFrame"
        df = df.dissolve(by=by_column).reset_index()

    logger.log("VERBOSE", "Converting polygons to raster")
    func = joblib.delayed(lambda ser, i: polygon_to_raster_bool(ser, da_target) * (i + 1))
    tasks = [func(row.geometry, i) for i, row in df.iterrows()]

    verbose_auto = True if logger.get_level() <= 10 else False
    props = dict(n_jobs=-1, verbose=verbose_auto, backend='threading')
    props.update(joblib_kwargs)
    
    polygons = joblib.Parallel(**props)(tasks)
    
    logger.log("VERBOSE", "Merging polygons into single raster")
    polygons = (
        xr.concat(polygons, dim='polygons')
        .assign_coords(polygons=df.index)
        .max(dim='polygons')
        .astype(int))

    return polygons


def raster_bool_to_vector(da: xr.DataArray, combine_polygons=False)->gpd.GeoDataFrame:
    """
    Converts a rasterized mask to a vectorized representation.

    Parameters
    ----------
    da : xr.DataArray (bool)
        The rasterized mask to convert to a polygon
    combine_polygons : bool, optional
        If True, all polygons are combined into a single polygon. if False, each
        connected component is a separate polygon. The default is False.

    Returns
    -------
    gpd.GeoDataFrame
        A GeoDataFrame with the vectorized representation of the mask.
    """
    import rasterio.features
    from shapely import geometry
    import numpy as np

    assert da.dtype == bool, "Input array must be boolean"
    assert da.ndim == 2, "Input array must be 2D"
    transform = da.rio.transform()
    
    arr = da.values.astype(np.uint8)
    shapes = rasterio.features.shapes(arr, transform=transform)

    get_coord = lambda s: geometry.Polygon(s[0]["coordinates"][0])
    polygons = [get_coord(shape) for shape in shapes if shape[1] == 1]

    if combine_polygons:
        polygons = [gpd.GeoDataFrame(geometry=polygons).unary_union]

    gdf = gpd.GeoDataFrame(geometry=polygons, crs=da.rio.crs)

    return gdf


def raster_int_to_vector(da: xr.DataArray, names=None)->gpd.GeoDataFrame:
    """
    Converts a rasterized mask with several classes to a vectorized representation.

    Parameters
    ----------
    da : xr.DataArray (int)
        The rasterized mask with several classes.
    names : list, optional
        The names of the classes. The default is None, in which case the classes are numbered.

    Returns
    -------
    gpd.GeoDataFrame
        A GeoDataFrame with the vectorized representation of the mask.
    """
    import numpy as np
    import pandas as pd

    assert da.dtype == int, "Input array must be integer"

    mask_values = np.sort(np.unique(da.values))
    n_classes = mask_values.size

    if n_classes > 20:
        raise ValueError("Too many classes to convert to vector")

    if names is None:
        names = [str(i) for i in mask_values]
    else:
        assert len(names) == n_classes, f"Number of names (n={len(names)}) must match number of classes (n={n_classes})"

    polygons = []
    for m, name in zip(mask_values, names):
        logger.log("VERBOSE", f"Converting class {name} [{m}] to vector")
        mask = da == m
        polygons += raster_bool_to_vector(mask, combine_polygons=True),
    
    polygons = pd.concat(polygons, ignore_index=True)
    polygons['class'] = names

    return polygons


def remove_small_objects(mask, min_size=10):
    """
    Remove small objects from a binary mask.

    Parameters
    ----------
    mask : xr.DataArray (bool)
        The binary mask to remove small holes or objects from 
    min_size : int, optional
        The minimum size of the objects to keep. The default is 10.

    Returns
    -------
    xr.DataArray
        A binary mask with small objects removed.
    """
    from skimage.morphology import remove_small_holes, remove_small_objects

    mask = mask.copy(deep=True)
    mask.values[:] = remove_small_objects(mask.values, min_size=min_size)
    mask.values[:] = remove_small_holes(mask.values, area_threshold=min_size)

    return mask

