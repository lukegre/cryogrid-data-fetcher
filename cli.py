import click
from cryogrid_data_fetcher import logger


@click.command()
@click.argument("fname_config", type=click.Path(exists=True))
def get_era5_data(fname_config):
    from cryogrid_data_fetcher.era5.from_weatherbench import download_era5_from_weatherbench
    from cryogrid_data_fetcher.config.loader import load_config_yaml

    config = load_config_yaml(fname_config)
    config = update_period_from_env(config)

    download_era5_from_weatherbench(config)


def update_period_from_env(config):
    import os

    start_year = os.environ.get("START_YEAR", None)
    end_year = os.environ.get("END_YEAR", None)

    if start_year is not None:
        logger.info(f"Overriding start_year from env: {start_year}")
        config["start_year"] = int(start_year)
    if end_year is not None:
        logger.info(f"Overriding end_year from env: {end_year}")
        config["end_year"] = int(end_year)

    return config


if __name__ == "__main__":

    get_era5_data()
