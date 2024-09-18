import xarray as xr
from loguru import logger


def decompose_decimal_day(matlab_date):
    days = int(matlab_date)
    frac = matlab_date - days
    hours = int(frac * 24)
    frac -= hours / 24
    minutes = int(frac * 60)
    frac -= minutes / 60
    seconds = frac * 60

    dt = dict(days=days, hours=hours, minutes=minutes, seconds=seconds)
    return dt


def matlab2datetime(matlab_date, t0="0001-01-01", offset_days=-367):
    import pandas as pd
    
    dt = decompose_decimal_day(matlab_date)
    dt = pd.DateOffset(**dt)

    t0 = pd.Timestamp(t0)

    offset = decompose_decimal_day(offset_days)
    offset = pd.DateOffset(**offset)

    time = t0 + dt + offset
    return time


def read_mat_struct_flat_as_dict(fname):
    from scipy.io import loadmat

    raw = loadmat(fname)
    key = [k for k in raw.keys() if not k.startswith('_')][0]
    array_with_names = raw[key][0]

    names = array_with_names.dtype.names
    arrays = [a.squeeze() for a in array_with_names[0]]
    data = {k: v for k, v in zip(names, arrays)}

    return data


def read_cryogrid_spatial_run_info(fname):
    """
    Reads in a custom saved item that was created with post_process_clusters.m

    Data read in is a flat struct object that contains an array called 
    `cluster_idx` and the rest does not have to be defined but must be
    the same length. `cluster_idx` does not conform to this restriction

    Parameters
    ----------
    fname: str
        the name of the file

    Returns
    -------
    tuple (xr.Dataset, xr.Dataset):
        0: a dataset containing the centroids of the clusters. 
           The index represents the index of the centroid as found in 
           [1]
        1: a dataset with flattened spatial data. The index is important
           to find the centroid information. Contains other 2D spatial 
           data. 
    """
    import pandas as pd
    
    data = read_mat_struct_flat_as_dict(fname)
    cluster_idx = data.pop('cluster_idx')
    df = pd.DataFrame.from_dict(data)
    df.index += 1

    centroids = (
        df
        .loc[cluster_idx]
        .set_index(cluster_idx)
        .to_xarray())
    
    df = df.to_xarray()

    return centroids, df


def read_OUT_regridded_FCI2_file(fname):
    import xarray as xr

    centroid_index = int(fname.split('_')[-2])

    data = read_mat_struct_flat_as_dict(fname)
    
    elevation = data.pop('depths').squeeze()
    time = data.pop('timestamp').squeeze()
    coords = {'time': time}

    ds = xr.Dataset()
    for key in data:
        arr = data[key]
        ds[key] = xr.DataArray(
            data=arr,
            dims=('level', 'time'),
            coords=coords)
    
    ds['elevation'] = xr.DataArray(data=elevation, dims=('level',))
    ds = ds.chunk({})
    ds = ds.expand_dims(index=[centroid_index])
    ds = ds.isel(time=slice(0, -1))
        
    return ds


def read_OUT_regridded_FCI2_parallel(glob_fname, **joblib_kwargs):
    from glob import glob
    import xarray as xr
    import joblib

    flist = sorted(glob(glob_fname))
    
    joblib_props = dict(n_jobs=-1, backend='threading', verbose=1)
    joblib_props.update(joblib_kwargs)
    
    func = joblib.delayed(read_OUT_regridded_FCI2_file)
    tasks = [func(f) for f in flist]
    worker = joblib.Parallel(**joblib_props)
    
    output = worker(tasks)

    ds = xr.combine_by_coords(output).astype('float32')

    time_matlab = ds.time.to_series()
    time_pandas = time_matlab.apply(matlab2datetime)
    ds = ds.assign_coords(time=time_pandas)

    return ds


def read_OUT_regridded_FCI2_TopoSub(fname_outputs_mat, fname_spatial_mat):

    centroids, spatial = read_cryogrid_spatial_run_info(fname_spatial_mat)
    ds = read_OUT_regridded_FCI2_parallel(fname_outputs_mat)

    crygrid_idx = set(ds.index.values)
    centroids_idx = set(centroids.index.values)
    missing_indicies = list(centroids_idx.difference(crygrid_idx))

    # return ds, centroids, spatial
    ds = ds.reindex(index=centroids.index)
    if len(missing_indicies) > 0:
        logger.warning(f"Missing indicies: n = {len(missing_indicies)}; check the CryoGrid run log files to look for failed clusters")

    # if loading multiple years, then elevation has a fictitious time dimension
    if 'time' in ds.elevation.dims:
        logger.debug("Removing ficticious time dimension from elevation")
        ds['elevation'] = ds.elevation.mean('time')

    depth = ds.elevation - centroids.elevation
    # check all values of depth along the "index" dimension are the same
    max_std = depth.std('index').max().compute().values
    error_msg = f"Depth is not constant along index (max = {max_std:.04g})"
    assert max_std < 5e-2, error_msg

    depth = depth.mean('index').compute()
    ds = ds.assign_coords(level=depth).rename(level='depth')

    index = ds.index.copy(deep=True)
    ds = ds.assign(index=centroids.cluster_num).rename(index='cluster')

    return ds, centroids, spatial


def read_stratigraphy_labels(fname_excell, pointer_label_col1="STRATIGRAPHY_STATVAR", row_offset=-1, col_offset=0):
    """
    Read stratigraphy labels from a CryoGrid Excel file. 

    Stratigraphy labels are entered as comments to the pointer cells. 
    The pointer cell is always in column 1. The labels are then relative to the pointer cell. 
    The default configuration works with the CryGrid demo file for Gaustatoppen. 

    Parameters
    ----------
    fname_excell : str
        Path to the Excel file
    pointer_label_col1 : str
        The label of the pointer cell in column 1 - labels will should 
        be located relative to the pointer cell.
    row_offset : int
        The row offset from the pointer cell to the label cell.
    col_offset : int
        The column offset from the pointer cell to the label cell.

    Returns
    -------
    labels : list
        A list of the cell contents that contain the stratigraphy labels.
    """
    import openpyxl

    file = openpyxl.open(fname_excell)
    worksheet = file.worksheets[0]  # always assume the first worksheet

    # find all the rows that contain the pointer label
    keep_rows = []
    for row in worksheet.iter_rows():
        cell0 = row[0]  # column 1 from file
        value = cell0.value
        # find the rows that contain the pointer
        if value == pointer_label_col1:
            # offset the row with the row_offset
            keep_rows += cell0.row + row_offset,

    # pointer is always in the first column, but the label can 
    # be in any column relative to the pointer but is constant
    col = 1 + col_offset
    # iterate through the rows and get the labels
    labels = [worksheet.cell(row=r, column=col).value for r in keep_rows]

    return labels

    
class CryoGrid_TopoSub:
    def __init__(self, fname_profiles:str, fname_spatial:str) -> None:
        from glob import glob
        import xrspatial

        self.fname_spatial = fname_spatial
        self.fname_profiles = fname_profiles

        flist = sorted(glob(fname_profiles))
        self.flist_profiles = flist
        logger.info(f"Reading {len(flist)} files in parallel")
    
        data, centroids, spatial_flat = read_OUT_regridded_FCI2_TopoSub(
            fname_profiles, fname_spatial)
        
        self.data = data
        self.centroids = centroids
        self.spatial_flat = spatial_flat

        for key in self.data.data_vars:
            setattr(self, key, self.data[key])

        self.spatial_mapped = xr.Dataset()
        self.spatial_mapped['elevation'] = self.get_mapped(spatial_flat.elevation)
        self.spatial_mapped['hillshade'] = xrspatial.hillshade(self.spatial_mapped.elevation, azimuth=0)  # azimuth=0 -> south
        self.spatial_mapped['cluster_num'] = self.get_mapped(spatial_flat.cluster_num)
        
        logger.debug("Data stored in `self.data`, `self.centroids`, `self.spatial_flat`, `self.spatial_mapped`")
    
    def __repr__(self) -> str:
        txt = f"CryoGrid_TopoSub(\n\t{self.fname_profiles}\n\t{self.fname_spatial})\n"

        txt += "\nDATA VARIABLES: "
        txt += str(tuple(self.data.sizes.keys())) + "\n"

        max_key_len = max([len(k) for k in self.data.data_vars]) + 1
        for key in self.data.data_vars:
            placeholder = f"{{key:>{max_key_len}}}".format(key=key)
            txt += f"{placeholder} - {self.data[key].shape}\n"
        
        return txt
    
    def __getitem__(self, key):
        if isinstance(key, str):
            return self.data[key]
        elif isinstance(key, int):
            return self.data.sel(cluster=key)
    
    def get_spatialized(self, vals_1D_clusters: xr.DataArray):

        logger.info('Using `cryo.spatial_flat.cluster_num` to spatialize data')
        da_in = vals_1D_clusters.compute()
        da_out = da_in.sel(cluster=self.spatial_flat.cluster_num)

        return da_out
    
    def get_mapped(self, vals_1D, x_name='coord_x', y_name='coord_y'):
        name = vals_1D.name

        if 'cluster' in vals_1D.dims:
            da = self.get_spatialized(vals_1D)
        else:
            da = vals_1D

        df = self.spatial_flat[[y_name, x_name]].to_dataframe()
        df = df.assign(**{name: da.values}).set_index([y_name, x_name])

        ds = df.to_xarray().rename(**{x_name: 'x', y_name: 'y'})
        logger.info(f"Mapping 1D data ({name}) to 2D [y={ds.y.size}, x={ds.x.size}] ")
        da = ds[name]

        return da
    
    def get_extent(self, pad=0):
        import numpy as np

        ds = self.spatial_mapped
        w = ds.x.min().values - pad
        e = ds.x.max().values + pad
        s = ds.y.min().values - pad
        n = ds.y.max().values + pad

        bbox = np.array([w, s, e, n])

        return bbox
    