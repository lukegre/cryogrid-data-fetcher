import fsspec
import xarray as xr
import pathlib 
from functools import wraps as _wraps
from .. import logger
from ..utils.s3_helpers import is_safe_s3_path


def register_dask_progressbar_based_on_logger(log_level_thresh=20):
    """
    Register Dask progress bar based on the logger level.

    Parameters
    ----------
    log_level_thresh : int, optional
        Log level threshold for displaying the progress bar. Default is 20 (INFO).

    Returns
    -------
    bool
        True if the progress bar is registered, False otherwise.
    """
    from dask.diagnostics import ProgressBar

    handlers = logger._core.handlers.values()
    log_levels = [hdl.levelno for hdl in handlers]
    log_level = min(log_levels)

    if hasattr(logger, "cb"):
        return True
    
    if log_level <= log_level_thresh:
        from tqdm.dask import TqdmCallback

        logger.info("Registering Dask progress bar")
        logger.cb = TqdmCallback(desc="Xarray processing")
        logger.cb.register()
        return True 
    else:
        return False


@xr.register_dataset_accessor("s3")
class S3io:
    def __init__(self, xarray_obj: xr.Dataset):
        self._obj = xarray_obj

        self.fs = fsspec.filesystem("s3")

    @_wraps(xr.Dataset.to_zarr)
    def to_zarr(self, fpath_s3: str, **kwargs)-> fsspec.mapping.FSMap:

        is_safe_s3_path(fpath_s3)
        assert fpath_s3.endswith('.zarr'), f"Path should end with '.zarr': {fpath_s3}"
        
        mapper = self.fs.get_mapper(fpath_s3)
        self._obj.to_zarr(mapper, **kwargs)

        logger.success(f"Data written to {fpath_s3}")
        
        return mapper
    
    def to_netcdf(self, s3_path: str, local_dst=None, **kwargs)-> fsspec.spec.AbstractBufferedFile:
        """
        Write the data to a NetCDF file on S3 bucket.

        Parameters
        ----------
        s3_path : str
            S3 path to write the data to.
        local_dst : str, optional
            Local path to write the data to, if None (default), a temporary file 
            is created that is deleted after the data is written to S3. Otherwise,
            the file is not deleted.
        **kwargs : dict
            Additional keyword arguments passed to xarray.Dataset.to_netcdf.

        Returns
        -------
        fsspec.spec.AbstractBufferedFile
            File object pointing to the file on S3.
        """
        import tempfile

        is_safe_s3_path(s3_path)
        assert s3_path.endswith('.nc'), f"Path should end with '.nc': {s3_path}"
        
        # if local_dst is None, create a temporary file, otherwise open the file
        if local_dst is None:
            tmp_file = tempfile.NamedTemporaryFile(delete=True)
            logger.debug(f"Writing to temporary file: {tmp_file.name}")
            local_dst = tmp_file.name
        else:
            tmp_file = open(local_dst, 'wb')

        # write the data to the temporary file
        self._obj.to_netcdf(local_dst, **kwargs)

        # copy the file to S3 bucket using fsspec
        self.fs.put_file(local_dst, s3_path)

        # close the temporary file - will be deleted if local_dst is None
        tmp_file.close()

        logger.success(f"Data written to {s3_path}")

        # return the file object
        file_s3 = self.fs.open(s3_path)
        return file_s3
    
    @staticmethod
    def open_mfdataset(s3_name, local_cache=None, **kwargs):
        """
        Open a dataset from multiple files on S3.

        Notes
        -----
        A progressbar for loading the data will be displayed if the tqdm package is installed 
        and 

        Parameters
        ----------
        s3_name : str or list
            S3 path or list of S3 paths to the files. If the path contains a * character,
            it is treated as a glob pattern. If it ends on a / character, it is treated as
            a directory and all .nc files in the directory are opened. If a list is provided,
            all files in the list are opened. Failure to meet these conditions will raise
            an error.
        local_cache : str, optional
            Local cache directory to store the files. If None (default), no local cache
            is used. If a string is provided, the files are cached in the directory.
            If the directory does not exist, an error is raised.
        progressbar : bool, optional
            If True, a progressbar is displayed while loading the data. Default is True.

        Returns
        -------
        xarray.Dataset
            Dataset created from the files

        Raises
        ------
        FileNotFoundError
            If the local cache directory does not exist.
        ValueError
            If the s3_name does not meet the conditions described above.
        """

        if local_cache is None:
            logger.warning("No local cache directory provided, opening S3 files without caching may be slower")
            desc = "Opening S3 files (no local cache)"
            fs = fsspec.filesystem("s3")
        elif isinstance(local_cache, str) and pathlib.Path(local_cache).exists():
            desc = "Downloading S3 files to local cache"
            fs = fsspec.filesystem(
                "filecache", 
                target_protocol="s3",
                cache_storage=local_cache,
                same_names=True)
        elif isinstance(local_cache, str) and not pathlib.Path(local_cache).exists():
            raise FileNotFoundError(f"Local cache directory does not exist: {local_cache}")
        
        if isinstance(s3_name, (list, tuple)):
            s3_paths = s3_name
        elif isinstance(s3_name, str):
            if '*' in s3_name and s3_name.endswith('/'):
                raise ValueError("s3_name cannot contain * and end with a / character")
            elif '*' in s3_name:
                s3_paths = fs.glob(s3_name)
            elif s3_name.endswith('/'):
                s3_paths = fs.glob(s3_name + "*.nc")
            else:
                if fs.isdir(s3_name):
                    s3_paths = fs.glob(s3_name + "/*.nc")
                else:
                    raise ValueError("s3_name should contain * or be a directory")
        else:
            raise ValueError("s3_name should be a string or a list of strings")
        
        if register_dask_progressbar_based_on_logger():
            from tqdm.auto import tqdm
            s3_paths = tqdm(s3_paths, desc=desc)

        s3_flist = [fs.open(s3_path) for s3_path in s3_paths]
        
        props = dict(parallel=True)
        props.update(kwargs)
        ds = xr.open_mfdataset(s3_flist, **props)

        return ds


def coord_0d_to_attrs(ds):
    attrs = {}
    for coord in ds.coords:
        if ds[coord].shape == ():
            val = ds[coord].values
            try:
                val = str(val)
                attrs[coord] = val
            except:
                pass
    ds.attrs.update(attrs)
    ds = ds.drop_vars(attrs.keys())
    return ds
