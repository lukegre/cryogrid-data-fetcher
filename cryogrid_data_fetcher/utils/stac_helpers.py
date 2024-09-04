
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
