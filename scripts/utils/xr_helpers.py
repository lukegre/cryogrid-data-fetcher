import fsspec
import xarray as xr
from functools import wraps as _wraps
from .. import logger
from ..utils.s3_helpers import is_safe_s3_path


def register_dask_progressbar_based_on_logger(log_level_thresh=10):
    from dask.diagnostics import ProgressBar

    handlers = logger._core.handlers.values()
    log_levels = [hdl.levelno for hdl in handlers]
    log_level = min(log_levels)
    
    if log_level <= log_level_thresh:
        ProgressBar().register()


@xr.register_dataset_accessor("s3")
class S3Accessor:
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

