import sys
from loguru import logger

from . import config
from . import era5
from . import dem
from . import utils
from . import cryogrid

from .era5.from_cds import download_era5_from_cds
from .era5.from_weatherbench import download_era5_from_weatherbench
from .dem.from_stac import main as download_dem_from_planetary_computer
from .snow.from_stac_s2msi import main as download_snow_index_from_planetary_computer
from .utils.helpers import change_logger_level, get_loguru_level as _get_loguru_level

try:
    logger.level(name='IMPORTANT', no=25, color='<r>', icon='🗣️')
except:
    pass
try:
    logger.level(name='VERBOSE', no=15, color='<lk>', icon='🗣️')
    change_logger_level('VERBOSE')
    logger.get_level = _get_loguru_level
except TypeError as e:
    print(e)


from icecream import ic as print
print.configureOutput(prefix=f'Debug | ', includeContext=True)