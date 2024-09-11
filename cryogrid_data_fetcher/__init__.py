import sys
from loguru import logger

from . import config
from . import era5
from . import dem
from . import utils

from .era5.from_cds import download_era5_from_cds
from .era5.from_weatherbench import download_era5_from_weatherbench
from .dem.from_stac import main as download_dem_from_planetary_computer
from .dem import from_stac as dem
from .utils.helpers import change_logger_level

try:
    logger.level(name='VERBOSE', no=15, color='<black>', icon='🗣️')
    change_logger_level('VERBOSE')
except TypeError as e:
    print(e)