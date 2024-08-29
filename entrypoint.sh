#!/bin/bash

ls -laF /myhome/cryogrid/
echo ===========
ls -laF /home/mambauser/
/opt/conda/bin/python \
    /home/mambauser/cryogrid-era5-downloader/cli.py \
    /home/mambauser/cryogrid-era5-downloader/requests/cryogrid_pamir_region.yml
