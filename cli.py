import click


def get_dem_data(bbox_WSEN, s3_dst_dir):
    pass


@click.command()
@click.argument("fname_config", type=click.Path(exists=True))
@click.option("--dry-run", is_flag=True, default=False, show_default=True, help="Do not download data, only print the requests")
def get_era5_data(fname_config, dry_run):
    from scripts.era5.from_weatherbench import download_era5_from_weatherbench
    from scripts.config.loader import load_config_yaml

    config = load_config_yaml(fname_config)

    download_era5_from_weatherbench(config)


if __name__ == "__main__":

    get_era5_data()
