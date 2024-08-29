import sys
from loguru import logger

from . import config
from . import era5
from . import dem
from . import utils

from .era5.from_cds import download_era5_from_cds
from .era5.from_weatherbench import download_era5_from_weatherbench
from .dem.from_stac import main as get_dem_data
from .dem import from_stac as dem

logger.remove()
logger.add(sys.stderr, level="INFO")
