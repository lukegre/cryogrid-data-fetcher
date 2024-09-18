
def search_stac_items(url, collection, bbox, **kwargs)->list:
    import planetary_computer
    import pystac_client

    catalog = pystac_client.Client.open(
        url=url,
        modifier=planetary_computer.sign_inplace)

    search = catalog.search(
        collections=[collection],
        bbox=bbox, 
        **kwargs)

    items = search.item_collection()

    return items


def get_sentinel2_granules(bbox, start_date, end_date, assets=['SCL'], max_cloud_cover=30, epsg=32643, res=10):
    import stackstac

    items = search_stac_items(
        url="https://planetarycomputer.microsoft.com/api/stac/v1",
        collection="sentinel-2-l2a",
        bbox=bbox,
        datetime=f"{start_date}/{end_date}",
        query={"eo:cloud_cover": {"lt": max_cloud_cover}})

    da = stackstac.stack(
        items, 
        assets=assets, 
        epsg=epsg, 
        bounds_latlon=bbox,
        chunksize=2048,
        resolution=res)
    
    da = da.astype(('float32'))
    
    return da