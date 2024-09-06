#!/bin/bash

# make build
# make push

# This script submits mutliple downloads to RunAI to speed up data retrieval
# On the SDSC's RunAI setup, workloads are limited to downloading at ~70MB/s
# But multiple jobs can be run in parallel to increase the download speed

START=1968  # inclusive
END=2023  # inclusive
STEP=8  # 8 years per job
IMAGE=$(make image)

for START_YEAR in $(seq $START $STEP $END); do
    END_YEAR=$(($START_YEAR+7))
    
    runai submit spi-luke-era5-downloader-$START_YEAR-$END_YEAR \
        --cpu 2 \
        --memory 12G \
        --environment START_YEAR=$START_YEAR \
        --environment END_YEAR=$END_YEAR \
        --environment AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
        --environment AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
        --environment AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL} \
        --image $IMAGE
done
