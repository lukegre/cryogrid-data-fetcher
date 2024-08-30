#!/bin/bash

make build
make push

# This script submits mutliple downloads to RunAI to speed up data retrieval
# On the SDSC's RunAI setup, workloads are limited to downloading at ~70MB/s
# But multiple jobs can be run in parallel to increase the download speed

START=1960  # inclusive
END=2023  # inclusive
STEP=8  # 8 years per job

for START_YEAR in $(seq $START $STEP $END); do
    END_YEAR=$(($START_YEAR+7))
    
    runai submit spi-luke-era5-downloader-$START_YEAR-$END_YEAR \
        --cpu 2 \
        --memory 12G \
        --environment START_YEAR=$START_YEAR \
        --environment END_YEAR=$END_YEAR \
        --image lukegre/era5-downloader:latest
done
