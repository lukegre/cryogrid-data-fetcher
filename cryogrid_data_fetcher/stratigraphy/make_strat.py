import xarray as xr
import numpy as np
from .. import logger


def main(dem: xr.DataArray)->xr.DataArray:
    from ..utils.shp_helper import polygon_to_raster_bool, polygons_to_raster_int
    from . import get_geology_ussr, get_glaciers_rgi, get_land_cover_esa

    da_dem = dem
    da_dem_smooth = (
        da_dem
        .rolling(x=3, y=3, center=True, min_periods=1).median()
        .rolling(x=3, y=3, center=True, min_periods=1).mean()
        .rio.set_crs(dem.rio.crs)
        .rio.set_nodata(dem.rio.nodata))
    
    # df_geo = get_geology_ussr(da_dem, return_as_df=True)
    # da_geo = polygons_to_raster_int(df_geo, da_dem, by_column='rock_type')

    df_glaciers = get_glaciers_rgi(target_dem=da_dem)
    da_glaciers = polygon_to_raster_bool(df_glaciers, da_dem)

    da_land_cover = get_land_cover_esa(da_dem)

    da_stratigraphy = make_stratigraphy_classes(da_land_cover, da_glaciers, da_dem_smooth)

    return da_stratigraphy


def make_bin_edges_from_bin_centers(bin_centers):
    """
    Create bin edges from bin centers

    The inner bin edges are calculated as the average of the two adjacent bin centers.
    The outer bin edges are calculated as x0 += ((x0 - x1) / 2)

    Parameters
    ----------
    bin_centers : np.array
        The bin centers of the histogram
    
    Returns
    -------
    np.array
        The bin edges of bin centers
    """
    bin_edges = np.convolve(bin_centers, [0.5, 0.5], mode='valid')
    x0 = bin_centers[0] - (bin_centers[1] - bin_centers[0]) / 2
    x1 = bin_centers[-1] + (bin_centers[-1] - bin_centers[-2]) / 2
    bin_edges = np.r_[[x0], bin_edges, [x1]]
    return bin_edges


def plot_classes(da: xr.DataArray, **kwargs):
    """
    Plot a classified DataArray with colorbar and class descriptions

    Parameters
    ----------
    da : xr.DataArray
        A classified DataArray with class values as integers. Must have the 
        following attributes: 'class_colors', 'class_values', 'class_descriptions'.
    **kwargs : dict
        Additional keyword arguments to pass to the plotting function.

    Returns
    -------
    fig, ax, img : tuple
        The figure, axes, and image object of the plot.
    """
    import matplotlib.pyplot as plt

    assert 'class_colors' in da.attrs, "DataArray must have 'class_colors' attribute"
    assert 'class_values' in da.attrs, "DataArray must have 'class_values' attribute"
    assert 'class_descriptions' in da.attrs, "DataArray must have 'class_descriptions' attribute"

    size_aspect = ('size' in kwargs) or ('aspect' in kwargs)
    print(size_aspect)
    if 'ax' not in kwargs and not size_aspect:
        fig, ax = plt.subplots(figsize=(12, 6), dpi=150)
        kwargs['ax'] = ax
    
    # getting the class values and colors
    colors = da.class_colors
    cmap = plt.cm.colors.ListedColormap(colors)

    # getting the bin edges
    bin_centers = np.array(da.class_values)
    bin_edges = make_bin_edges_from_bin_centers(bin_centers)

    # setting up plotting defaults
    props = dict(levels=bin_edges, cmap=cmap, cbar_kwargs=dict(pad=0.01))
    props.update(kwargs)  # allow user to override defaults
    # plotting the data
    img = da.plot.imshow(**props)

    # getting the figure, axes and colorbar
    fig, ax, cbar = img.figure, img.axes, img.colorbar
    
    # setting the colorbar properties
    ticks = np.convolve(bin_edges, [0.5, 0.5], mode='valid')
    labels = [f"{i: >2} = {s}" for i, s in zip(bin_centers, da.class_descriptions)]
    cbar.set_ticks(ticks, labels=labels, fontsize=9, fontfamily='monospace')
    cbar.ax.tick_params(length=0)
    cbar.set_label('')

    fig.tight_layout()

    ax.set_title('')
    ax.set_aspect('equal')

    return fig, ax, img


def make_stratigraphy_classes(land_cover:xr.DataArray, glacier_mask:xr.DataArray, elevation:xr.DataArray, slope_threshold=30):
    """
    Create a stratigraphy mask from land cover, glacier mask, and elevation data

    The core function of stratigraphies. The main input is the ESA world cover dataset. 
    The function uses the land cover data to classify the terrain into different classes
    based on the land cover, slope, and glacier mask. The classes are then combined into
    a single stratigraphy mask. The classes are as follows:
        1: excluded      crops, built-up, water, forests, herbaceous wetland
        2: glaciers      RGI mask (also excluded from CryoGrid runs)
        3: bedrock       bare rock/soil with slope > 30° 
        4: bare_soil     bare rock/soil with slope ≤ 30°
        5: vegetation    shrubland, grassland (assume thicker soils)
        6: lichen_moss   lichen and moss (assume thinner soils or tallus)

    Parameters
    ----------
    land_cover : xr.DataArray [int]
        The land cover data from ESA world cover (values=10:10:100)
    glacier_mask : xr.DataArray [bool]
        The glacier mask from the RGI dataset 
    elevation : xr.DataArray [float]
        The elevation data from a DEM used to calculate slope. Note that 
        all data must be on the same grid and have the same CRS.

    Returns
    -------
    xr.DataArray [int]
        A stratigraphy mask with the classes as defined in the description above. 
        Contains attributes 'class_values', 'class_descriptions', 'class_colors' for plotting.
    """
    from ..dem.derived import slope as calc_slope
    from ..utils.xr_helpers import drop_non_index_coords

    val_exclude = [10, 40, 50, 80, 90]  # crops, built-up, water, herbaceous wetland
    val_bare_ground = [60, 70]  # bare/sparse + snow/ice
    val_vegetated = [20, 30]  # shrubland, grassland
    val_lichen_moss = [100]  # lichen and moss

    slope = calc_slope(elevation)
    land_cover = land_cover.pipe(drop_non_index_coords)
    space = '\n     '  # used by locals() later, so don't remove

    strat_dict = {
        'excluded': {
            'data': land_cover.isin(val_exclude),
            'description': 'Excluded{space}crops, built-up, water, forsests', 
            'color': '#FA0000', 
            'value': 1},
        'glaciers': {
            'data': glacier_mask,
            'description': 'Glacier{space}RGI mask', 
            'color': '#fcfcfc', 
            'value': 2},
        'bedrock': {
            'data': land_cover.isin(val_bare_ground) & (slope > slope_threshold),
            'description': 'Bed-rock{space}slope > {slope_threshold}°', 
            'color': '#485b73', 
            'value': 3},
        'bare_soil': {
            'data': land_cover.isin(val_bare_ground) & (slope <= slope_threshold),
            'description': 'Bare soil{space}slope ≤ {slope_threshold}°', 
            'color': '#c4b18b', 
            'value': 4},
        'vegetation': {
            'data': land_cover.isin(val_vegetated),
            'description': 'Vegetated{space}thicker soils', 
            'color': '#FFFF4C', 
            'value': 5},
        'lichen_moss': {
            'data': land_cover.isin(val_lichen_moss),
            'description': 'Lichen and moss{space}rocky/thin soil', 
            'color': '#469ec7', 
            'value': 6}
        }

    mask = xr.concat([info['data'] for info in strat_dict.values()], dim='class').compute()
    check_stratigraphy_masks(mask)
    check_stratigraphy_classes(strat_dict)
    
    stratigraphy = land_cover.astype(float) * np.nan
    descriptions = []
    for key, info in strat_dict.items():
        logger.debug(f"Adding `{key}` to stratigraphy")

        data = info['data'].astype(float) * info['value']  # mask with value
        mask = data.astype(bool) & stratigraphy.isnull()  # only where we haven't already set 
        data_masked = data.where(mask)
        stratigraphy = stratigraphy.fillna(data_masked)

        # string placeholders not in namespace of list comprehension, thus define here
        descriptions += info['description'].format(**locals()),  

    classes = strat_dict.values()
    stratigraphy = (
        stratigraphy
        .astype(int)
        .assign_attrs(
            long_name='Estimated stratigraphy',
            description=(
                'Stratigraphy estimated from land cover, slope, and glacier mask '
                'Bare rock and soil are distinguished by slope threshold of '
                '{slope_threshold} degrees.').format(**locals()),
            class_values=[d['value'] for d in classes],
            class_descriptions=descriptions,
            class_colors=[d['color'] for d in classes])
        .rio.set_crs(elevation.rio.crs)
        .rio.set_spatial_dims(x_dim='x', y_dim='y'))

    return stratigraphy


def check_stratigraphy_classes(classes: dict):
    """
    Check if the stratigraphy classes are correctly formatted and have the correct data

    Parameters
    ----------
    classes : dict
        A dictionary with the stratigraphy classes. Each key is a class name and the 
        value is a dictionary with the following keys: 'data', 'description', 'color', 'value'
    
    Raises
    ------
    AssertionError
        If the classes are not correctly formatted - information is given about the error
    """

    for key, info in classes.items():
        assert info['data'].ndim == 2, f"FAILED CHECK: Data for `{key}` must be 2D"
        assert info['data'].dtype == np.bool_, f"FAILED CHECK: Data for `{key}` must be boolean"
        assert info['data'].shape == classes['excluded']['data'].shape, f"FAILED CHECK: Data for `{key}` must have same shape as 'excluded'"

        assert 'description' in info, f"FAILED CHECK: Description for `{key}` must be defined"
        assert 'color' in info, f"FAILED CHECK: Color for `{key}` must be defined"
        assert 'value' in info, f"FAILED CHECK: Value for `{key}` must be defined"

        assert isinstance(info['description'], str), f"FAILED CHECK: Description for `{key}` must be a string"
        assert isinstance(info['color'], str), f"FAILED CHECK: Color for `{key}` must be a string"
        assert isinstance(info['value'], int), f"FAILED CHECK: Value for `{key}` must be an integer"
        assert info['value'] > 0, f"FAILED CHECK: Value for `{key}` must be greater than 0. Cannot be 0 for any stratigraphy class"

    logger.debug('PASSED CHECKS: stratigraphy class dictionary and all classes have correct format')


def check_stratigraphy_masks(mask: xr.DataArray):
    """
    Check if the stratigraphy mask is correctly formatted and has the correct data

    Stratigraphy masks must have the following properties:
        3D with dimensions ['class', 'y', 'x']
        Dtypes must be boolean
        No overlapping classes
        No unclassified pixels

    Parameters
    ----------
    mask : xr.DataArray [bool]
        A stratigraphy mask with a dimension 'class', where each class is a boolean mask

    Raises
    ------
    AssertionError
        If the mask is not correctly formatted - information is given about the error
    """

    assert mask.ndim == 3, "FAILED CHECK: Input must be 3D [class, y, x]"
    assert list(mask.dims) == ['class', 'y', 'x'], "FAILED CHECK: Input must have dims ['class', 'y', 'x']"
    logger.debug('PASSED CHECKS: dimensions must be [class, y, x]')

    assert mask.dtype == np.bool_, "FAILED CHECK: Input must be boolean"
    logger.debug('PASSED CHECKS: dtypes must be bool')

    multiple_classes = mask.sum(dim='class') != 1
    unclassified = mask.any(dim='class')

    n_multi_class = multiple_classes.sum().values
    n_unclassified = unclassified.sum().values

    assert n_multi_class != 0, f"FAILED CHECK: Some pixel classes overlap (n_pixels={n_multi_class})"
    assert n_unclassified != 0, f"FAILED CHECK: Some pixels are not classified (n_pixels={n_unclassified})"
    logger.debug('PASSED CHECKS: mask cannot have overlapping classes or unclassified pixels')