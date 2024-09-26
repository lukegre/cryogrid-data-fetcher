import xdem as _xdem
from memoization import cached as _cached


_TERRAIN_FUNCS = _xdem.terrain.available_attributes

DEFAULT_PROPS = dict(
    use_richdem=True if _xdem.terrain._has_rd else False,
    degrees=True, # aspect degrees
    azimuth=90)  # hillshade azimuth

def _xdem_xarray_wrapper(func):
    from functools import wraps
    func_name = func.__name__

    @_cached(max_size=len(_TERRAIN_FUNCS))
    @wraps(func)
    def wrapper(*args, **kwargs):
        import numpy as np

        fargs = func.__code__.co_varnames
        props = {k: v for k, v in DEFAULT_PROPS.items() if k in fargs}
        props.update(kwargs)
        
        dem = _xdem.DEM.from_xarray(*args)
        dem = (
            func(dem, **props)
            .to_xarray(name=func_name)
            .squeeze()
            .assign_attrs(**{f"xdem__{k}": v for k, v in props.items()})
            .compute())

        return dem
    
    return wrapper


for func in _TERRAIN_FUNCS:
    locals()[func] = _xdem_xarray_wrapper(getattr(_xdem.terrain, func))
