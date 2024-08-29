# default variables
DOCKER_TAG ?= latest
DOCKER_NAME ?= era5-downloader
DOCKER_ENTRYPOINT ?= bash

CPUS ?= 2
RAM ?= 12G
PORT_SSH ?= 2022
ARCHITECHTURE ?= amd64
# $(shell uname -m)  # <- uses architecture of the host but fails to build rasterio

# ========================================================================
# DONT CHANGE ANYTHING BELOW THIS POINT UNLESS YOU KNOW WHAT YOU ARE DOING
# ========================================================================
DOCKER_USER=lukegre
RUNAI_PROJECT=spi-luke

# automated variables
DOCKER_PLATFORM=linux/${ARCHITECHTURE}
DOCKER_IMAGE=${DOCKER_USER}/${DOCKER_NAME}:${DOCKER_TAG}
RUNAI_JOBNAME=runai-${RUNAI_PROJECT}-${DOCKER_NAME}-${DOCKER_TAG}

# some makefile magic commands
.DEFAULT_GOAL := HELP
.PHONY: HELP


build:  ## Docker: build the docker image from ./Dockerfile
	docker build --platform=${DOCKER_PLATFORM} -t ${DOCKER_IMAGE} .

run:  ## Docker: run the docker container from ./Dockerfile
	docker run \
		--rm \
		--interactive \
		--tty \
		--volume ./.env:/myhome/cryogrid/.env \
		--entrypoint ${DOCKER_ENTRYPOINT} \
		${DOCKER_IMAGE} $(DOCKER_RUN_CMD)

push:  ## Docker: push the docker image to dockerhub
	@echo "Pushing ${DOCKER_IMAGE} to dockerhub: https://hub.docker.com/repository/docker/${DOCKER_IMAGE}/"
	docker push ${DOCKER_IMAGE}

runai-login:  ## RunAI: login to runai
	@runai login

runai-submit:  ## RunAI: submit the job to runai - also mounts the s3 bucket
	runai submit ${RUNAI_JOBNAME} \
		--cpu ${CPUS} \
		--memory ${RAM} \
		-i ${DOCKER_IMAGE} \
		--interactive

runai-status:  ## RunAI: get the status of the job
	runai describe job ${RUNAI_JOBNAME} -p ${RUNAI_PROJECT}

HELP:  # show this help
	@echo ENVIRONMENT VARIABLES
	@echo ========================
	@echo "DOCKER_IMAGE       ${DOCKER_IMAGE}"
	@echo "DOCKER_PLATFORM    ${DOCKER_PLATFORM}"
	@echo "DOCKER_ENTRYPOINT  ${DOCKER_ENTRYPOINT}"
	@echo ------------------------
	@echo "RUNAI_PROJECT      ${RUNAI_PROJECT}"
	@echo "RUNAI_JOBNAME      ${RUNAI_JOBNAME}"
	@echo ========================
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
