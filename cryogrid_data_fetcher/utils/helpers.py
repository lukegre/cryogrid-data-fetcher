import string
from .. import logger


class SafeFormatter(string.Formatter):
    """
    A string formatter that does not raise KeyError if a key is missing
    """
    def get_value(self, key, args, kwargs):
        if isinstance(key, str):
            return kwargs.get(key, f'{{{key}}}')
        else:
            return super(SafeFormatter, self).get_value(key, args, kwargs)


def resolve_format_strings(dct):
    """
    Resolve format strings in a dictionary based on 
    other keys in the dictionary

    Parameters
    ----------
    dct : dict
        A dictionary with format strings
    
    Returns
    -------
    dict
    """
    def _resolve_format_strings(dct, parent_namespace={}):

        namespace = {**parent_namespace, **dct}
        formatter = SafeFormatter()

        for key, value in dct.items():
            if isinstance(value, dict):
                _resolve_format_strings(value, namespace)
            elif isinstance(value, str):
                dct[key] = formatter.format(value, **namespace)
        return dct
    
    logger.debug("Resolving format strings")
    dct = _resolve_format_strings(dct)
    dct = _resolve_format_strings(dct)
    return dct


def change_logger_level(level):
    """
    Change the logger level of the logger

    Parameters
    ----------
    level : str or int
        The level to change the logger to
    """
    import sys
    
    logger.remove()
    logger.add(
        sys.stderr, 
        level=level, 
        colorize=True, 
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")
    
    logger.log(level, f"Logger level changed to {level}")

    if isinstance(level, str):
        if level == "DEBUG":
            level = 10
        elif level == "VERBOSE":
            level = 15
        elif level == "INFO":
            level = 20
        elif level == "WARNING":
            level = 30
        elif level == "ERROR":
            level = 40
        elif level == "CRITICAL":
            level = 50
        
        if level <= 15:
            from .xr_helpers import register_dask_progressbar_based_on_logger
            register_dask_progressbar_based_on_logger(level)
        elif level > 15 and hasattr(logger, "cb"):
            logger.info("Unregistering Dask progress bar")
            logger.cb.unregister()