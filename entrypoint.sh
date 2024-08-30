#!/bin/bash

# this command will be run when the docker container starts without any arguments
/opt/conda/bin/python \
    /home/mambauser/cryogrid-era5-downloader/cli.py \
    /home/mambauser/cryogrid-era5-downloader/requests/cryogrid_pamir_region.yml
