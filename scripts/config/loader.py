import munch as _munch
import pathlib as _pathlib

from .. import logger


_PWD = _pathlib.Path(__file__).parent
_SCHEMA_PATH = _PWD / "schema.yaml"


def load_config_yaml(fname_yaml)->_munch.Munch:
    """
    Load a YAML file that contains request information for a 
    CryoGrid run using the TopoSub approach. 
    """
    import yaml
    import pathlib 
    import dotenv
    from ..utils.dict_helpers import resolve_format_strings
    from ..utils.yml_helpers import validate_yaml_file

    validate_yaml_file(fname_yaml, filename_schema=_SCHEMA_PATH)
    
    with open(fname_yaml, 'r') as f:
        config = yaml.safe_load(f)

    config['fname_yaml'] = pathlib.Path(fname_yaml).resolve()
    config['bbox_str'] = make_bbox_str(config['bbox_WSEN'])

    # resolve format strings
    config = resolve_format_strings(config)
    config = _munch.munchify(config)

    check_era5_vars(config)
    check_s3_paths(config)

    dotenv_fname = dotenv.find_dotenv(raise_error_if_not_found=True)
    logger.success(f".env file found: {dotenv_fname}")

    dotenv.load_dotenv(dotenv_fname, verbose=True)

    logger.success(f"Loaded request from {fname_yaml}")

    return config


def make_template(filename_out:str, schema_fname=_SCHEMA_PATH):
    from ..utils.yml_helpers import make_template_from_schema

    make_template_from_schema(schema_fname, filename_out)
    logger.success(f"Config template written to {filename_out}")


def check_era5_vars(request):
    """
    Check if the request dictionary has the required keys for CryoGrid runs
    """
    check_era5_single_level(request)
    check_era5_pressure_level(request)


def check_s3_paths(request):
    from ..utils.s3_helpers import is_safe_s3_path

    is_safe_s3_path(request['fpath_base_s3'])
    is_safe_s3_path(request['dem']['fpath_s3'])
    is_safe_s3_path(request['era5']['dst_dir_s3'])


def check_era5_single_level(dct):
    logger.debug("Checking for compulsory ERA5 single-level variables")

    min_req_vars = [
        'surface_solar_radiation_downwards',
        'surface_thermal_radiation_downwards',
        'toa_incident_solar_radiation',
        'total_precipitation',
        '10m_u_component_of_wind',
        '10m_v_component_of_wind',
        'surface_pressure',
        '2m_dewpoint_temperature',
        '2m_temperature']
    
    vars = dct['era5']['single_levels']['variable']
    
    return _check_missing_values(vars, min_req_vars, name='era5.single_levels.variable')


def check_era5_pressure_level(dct):
    logger.debug("Checking for compulsory ERA5 pressure-level variables")

    min_req_vars = [
        'geopotential',
        'specific_humidity',
        'temperature',
        'u_component_of_wind',
        'v_component_of_wind']
    
    min_req_levels = '700 750 800 850 900 950 1000'.split()
    
    vars = dct['era5']['pressure_levels']['variable']
    lvls = dct['era5']['pressure_levels']['pressure_level']

    passes_values = _check_missing_values(vars, min_req_vars, name='era5.pressure_levels.variable')
    passes_levels = _check_missing_values(lvls, min_req_levels, name='era5.pressure_levels.pressure_level')

    return passes_values and passes_levels


def _check_missing_values(lst, required, name=None):
    missing = []
    for key in required:
        if key not in lst:
            missing.append(key)
    if len(missing) > 0:
        if name is None:
            raise KeyError(f"Missing required keys: {missing}")
        else:
            raise KeyError(f"Missing required keys in `{name}`: {missing}")
    return True


def make_bbox_str(bbox: list):

    def nice_coord(coord, compass_direction):
        if compass_direction.lower() in "we":
            direction = "E" if coord > 0 else "W"
        elif compass_direction.lower() in "ns":
            direction = "N" if coord > 0 else "S"
        else:
            raise ValueError("Invalid compass direction")
        # return f"{direction}{round(abs(coord)*100):.0f}"
        return f"{compass_direction}{round((coord)*100):.0f}"
    
    bbox_str = [nice_coord(b, c) for b, c in zip(bbox, "WSEN")]
    bbox_str = "_".join(bbox_str)

    return bbox_str
