from .. import logger
import geopandas as gp
from functools import lru_cache


def read_wfs(url:str, layer:str, bbox=None, **kwargs):

    logger.log("VERBOSE", f"Fetching data from URL: {url}")

    assert isinstance(layer, str), "Layer must be a string"
    bbox = _process_bbox(bbox)

    # Initialize
    df = _read_wfs(url, layer, bbox=bbox, **kwargs)

    return df


@lru_cache(10)
def _read_wfs(url, layer, bbox=None, **kwargs):
    from owslib.wfs import WebFeatureService

    wfs = WebFeatureService(url=url, **kwargs)

    if layer not in wfs.contents:
        message = f"Layer '{layer}' not found in the WFS service, available layers are: {', '.join(wfs.contents)}"
        raise ValueError(message)
    
    request = wfs.getfeature(
        typename=[layer],
        bbox=bbox,
        outputFormat='json')
    
    bytes = request.read()
    df = gp.read_file(bytes)

    return df


def _process_bbox(bbox):

    if bbox is None:
        logger.warning("No bounding box provided, will fetch all data")
        return bbox
    elif isinstance(bbox, (list, tuple)):
        bbox = tuple(bbox)
        assert len(bbox) == 4, "Bounding box must be a list or tuple of 4 elements"
    elif isinstance(bbox, (gp.GeoDataFrame, gp.GeoSeries)):
        bbox = tuple(bbox.total_bounds.tolist())
    else:
        raise ValueError("Bounding box must be a list or tuple of 4 elements")

    logger.log("VERBOSE", f"Getting data within the bounding box (WSEN): {bbox}")

    return bbox