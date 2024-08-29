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
