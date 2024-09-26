"""
sometimes the WFS service fails. In this case you can use the shapefile instead
Download the data from https://www.sciencebase.gov/catalog/item/60ad3d53d34e4043c850f275
"""

from .. import logger
import pandas as pd
import xarray as xr


WFS_URL = "https://www.sciencebase.gov/catalogMaps/mapping/ows/60ad3d53d34e4043c850f275?service=wfs&request=getcapabilities&version=1.0.0"
WFS_LAYER = "sb:geo1ec"


def geology_df_to_xarray(df: pd.DataFrame, dem_grid: xr.DataArray):
    from ..utils.shp_helper import polygon_to_raster_bool
    polygons = []

    rock_types = df.rock_type.unique()
    for c, key in enumerate(rock_types):
        i = df.rock_type == key
        subset = df[i]
        polygons += polygon_to_raster_bool(subset, dem_grid) * (c + 1),

    polygons = (
        xr.concat(polygons, dim='rock_type')
        .assign_coords(rock_type=rock_types)
        .max(dim='rock_type'))
    
    return polygons


def read_ussr_geology_from_shapefile(path_shp:str, dem_grid: xr.DataArray, return_as_df=False):
    from ..utils.shp_helper import read_shapefile_and_clip_to_grid, polygons_to_raster_int

    df = read_shapefile_and_clip_to_grid(path_shp, target_grid=dem_grid)
    df = process_ussr_geology(df)

    if return_as_df:
        return df
    else:
        da = polygons_to_raster_int(df, dem_grid, by_column='rock_type')
        return da


def get_ussr_geology(dem_grid: xr.DataArray, url=WFS_URL, layer=WFS_LAYER, return_as_df=False, **kwargs):
    from ..utils.wfs_helper import read_wfs
    from ..utils.shp_helper import clip_geodata_to_grid

    logger.info("Fetching geological data from the former USSR")
    bbox_WSEN = dem_grid.rio.reproject("EPSG:4326").rio.bounds()
    df = read_wfs(url, layer, bbox=bbox_WSEN, **kwargs)

    df = clip_geodata_to_grid(df, dem_grid)
    df = df[~df.is_empty]
    df = process_ussr_geology(df)

    if return_as_df:
        return df
    else:
        da = geology_df_to_xarray(df, dem_grid)
        return da


def process_ussr_geology(df):
    df = df.rename(columns={"GLG": "key", "PERIMETER": "perimeter", "AREA": "area"})
    
    df['long_name'] = _geological_key.loc[df.key].values
    df['rock_type'] = _berock_types.loc[df.key].values
    
    return df

    

def show_legend(**kwargs):
    from IPython.display import Image
    from IPython.display import display

    props = dict(
        url=(
            "https://certmapper.cr.usgs.gov/server/services/geology/formersovietunion/MapServer/WMSServer?"
            "request=GetLegendGraphic%26"
            "version=1.3.0%26"
            "format=image/png%26"
            "layer=0")
    )
    props.update(kwargs)

    logger.info(f"Displaying the legend from URL: {props['url']}")
    display(Image(**props))


# used ChatGPT4o to generate the dictionary from the legend image
_geological_key = pd.Series({
    "A": "Archean",
    "C": "Carboniferous",
    "CD": "Carboniferous and Devonian",
    "Cm": "Cambrian",
    "CmPt": "Cambrian-Proterozoic",
    "D": "Devonian (undivided)",
    "DS": "Devonian and Silurian",
    "Lakes and wide rivers": "",
    "Arctic areas covered by Ice": "",
    "J": "Jurassic (undivided)",
    "JTr": "Jurassic and Triassic",
    "K": "Cretaceous (undivided)",
    "KJ": "Cretaceous and Jurassic",
    "Mi": "Acidic Mesozoic intrusive rocks",
    "N": "Neogene",
    "NPg": "Neogene and Paleogene",
    "O": "Ordovician",
    "OCm": "Ordovician-Cambrian",
    "P": "Permian",
    "PC": "Permian-Carboniferous",
    "Pz": "Paleozoic (undivided)",
    'PZ': "Paleozoic (undivided)", 
    "Pg": "Paleogene",
    "PgK": "Paleogene and Cretaceous",
    "Pi": "Paleozoic intrusive rocks",
    "Pt": "Proterozoic",
    "PtA": "Acidic Proterozoic and Archean intrusive rocks",
    'PtAi': "Acidic Proterozoic and Archean intrusive rocks",
    "Q": "Quaternary (undivided)",
    "QT": "Quaternary and Tertiary",
    "Qv": "Extrusive rocks Pliocene and Quaternary",
    "S": "Silurian",
    "Sea and large lakes": "",
    "SO": "Silurian-Ordovician",
    "TKi": "Acidic intrusive rocks Cretaceous, Paleogene and Neogene",
    "Tr": "Triassic",
    "TrP": "Triassic and Permian",
    "X": "Lower and Middle Proterozoic",
    "Y": "Upper and Middle Proterozoic",
    "Z": "Upper Proterozoic",
    "ii": "Basic, Ultrabasic and Alkaline intrusive rocks of unknown age",
    "Areas outside of the former Soviet Union": "",
    "pC": "Precambrian (undivided)",
    'SEA': "Ocean", 
    'oth': "Other",
    'Ice': "Ice sheet", 
    'H2O': "Water", 
})


_berock_types = pd.Series({
    "A": "granite",               # Archean - Known for ancient granites
    "C": "limestone",             # Carboniferous - Common marine limestone deposits
    "CD": "limestone",            # Carboniferous and Devonian - Marine limestones are common
    "Cm": "sandstone",            # Cambrian - sandstones are quite common
    "CmPt": "schist",             # Cambrian-Proterozoic - Contains metamorphic rocks like schist
    "D": "limestone",             # Devonian - Marine limestones are dominant
    "DS": "limestone",            # Devonian and Silurian - Often marine limestone
    "J": "limestone",             # Jurassic - Known for limestone
    "JTr": "sandstone",           # Jurassic and Triassic - Contains sandstone
    "K": "sandstone",             # Cretaceous - sandstone is common in deposits
    "KJ": "sandstone",            # Cretaceous and Jurassic - sandstone is common
    "Mi": "granite",              # Acidic Mesozoic intrusive rocks - Likely granite
    "N": "sandstone",             # Neogene - Commonly contains sandstone
    "NPg": "limestone",           # Neogene and Paleogene - Contains limestone
    "O": "limestone",             # Ordovician - Marine limestones are common
    "OCm": "limestone",           # Ordovician-Cambrian - Primarily limestone and sandstone - periods are known for extensive sedimentary deposits, especially marine limestone 
    "P": "limestone",             # Permian - Contains limestone in marine environments
    "PC": "limestone",            # Permian-Carboniferous - Marine limestone deposits
    "Pz": "sandstone",            # Paleozoic (undivided) - Limestone, sandstone, and shale are more typical for most of the Paleozoic, especially given the marine transgressions during this era.
    'PZ': "sandstone",            # Paleozoic (undivided) - Limestone, sandstone, and shale are more typical for most of the Paleozoic, especially
    "Pg": "limestone",            # Paleogene - Includes limestone
    "PgK": "limestone",           # Paleogene and Cretaceous - limestone is common
    "Pi": "granite",              # Paleozoic intrusive rocks - Likely granite
    "Pt": "schist",               # Proterozoic - Metamorphic schists are common
    "PtA": "granite",             # Acidic Proterozoic and Archean intrusive rocks - Includes granite
    'PtAi': "granite",            # Acidic Proterozoic and Archean intrusive rocks - Includes granite
    "Q": "sandstone",             # Quaternary - Known for sands and sandstones
    "QT": "sandstone",            # Quaternary and Tertiary - Contains sandstones
    "Qv": "sandstone",            # Extrusive rocks Pliocene and Quaternary - sandstone and volcanic rocks
    "S": "limestone",             # Silurian - Common marine limestones
    "SO": "limestone",            # Silurian-Ordovician - Contains marine limestones
    "TKi": "granite",             # Acidic intrusive rocks from Cretaceous, Paleogene, and Neogene - Likely granite
    "Tr": "sandstone",            # Triassic - Known for sandstone
    "TrP": "sandstone",           # Triassic and Permian - Contains sandstone
    "X": "schist",                # Lower and Middle Proterozoic - Metamorphic rocks like schist
    "Y": "schist",                # Upper and Middle Proterozoic - Likely to include schist
    "Z": "schist",                # Upper Proterozoic - Contains schist
    "ii": "granite",              # Basic, Ultrabasic and Alkaline intrusive rocks of unknown age - Likely granite
    "pC": "granite",              # Precambrian - Known for granite
    "Sea and large lakes": "",    # Not classified
    "Lakes and wide rivers": "",  # Not classified
    "Arctic areas covered by Ice": "",  # Not classified
    "Areas outside of the former Soviet Union": "",  # Not classified
    "Ice": "",                    # Not classified
    "H2O": "",                    # Not classified
    "SEA": "",                    # Not classified
    "oth": ""                     # Not classified
})