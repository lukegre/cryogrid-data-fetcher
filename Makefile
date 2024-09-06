
# variables with defaults that can be changed in the command line
DOCKER_TAG ?= latest
CPUS ?= 2
RAM ?= 12G

# you can change these variables, but better to change with .env file
DOCKER_USER ?= 
RUNAI_PROJECT ?= 


# ==================================================
# RECOMMENDED TO NOT CHANGE ANYTHING BELOW THIS LINE
# ==================================================
# get docker name from parent directory name
DOCKER_NAME ?= $(shell basename $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST)))))
# docker entrypoint defaults to /entrypoint.sh - also image default
DOCKER_ENTRYPOINT ?= /entrypoint.sh

# the docker platform must be set to amd64 otherwise build fails and wont work on RunAI
DOCKER_PLATFORM=linux/amd64
# automated variables
DOCKER_IMAGE=${DOCKER_USER}/${DOCKER_NAME}:${DOCKER_TAG}
RUNAI_JOBNAME=${RUNAI_PROJECT}-${DOCKER_NAME}-${DOCKER_TAG}

# some makefile magic commands
.DEFAULT_GOAL := HELP
.PHONY: HELP

# include ENV variables in .env file if it exists
ifeq (,$(wildcard .env))
$(error ERROR: .env file not found - please create an .env file with S3 credentials)
else
include .env
endif

image: ## Docker - export the docker image name to the console
	@echo ${DOCKER_IMAGE}

build:  ## Docker - build the docker image from ./Dockerfile
	docker build --platform=${DOCKER_PLATFORM} -t ${DOCKER_IMAGE} .

bash:  ## Docker - lunch the container into bash
	@$(MAKE) run DOCKER_ENTRYPOINT=/bin/bash

run:  ## Docker - run the docker container from ./Dockerfile
	@docker run \
		--rm \
		--tty \
		--interactive \
		--env-file ./.env \
		--entrypoint ${DOCKER_ENTRYPOINT} \
		--name ${RUNAI_JOBNAME} \
		${DOCKER_IMAGE}

push:  ## Docker - push the docker image to dockerhub
	@echo "Pushing ${DOCKER_IMAGE} to dockerhub: https://hub.docker.com/repository/docker/${DOCKER_IMAGE}/"
	docker push ${DOCKER_IMAGE}

runai-login:  ## RunAI - login to runai
	@runai login

runai-submit:  ## RunAI - submit the job to runai - also mounts the s3 bucket
	@runai submit ${RUNAI_JOBNAME} \
		--cpu ${CPUS} \
		--memory ${RAM} \
		--environment AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
		--environment AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
		--environment AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL} \
		--image ${DOCKER_IMAGE}

runai-status:  ## RunAI - get the status of the job
	@runai describe job ${RUNAI_JOBNAME} -p ${RUNAI_PROJECT}

runai-logs:  ## RunAI - get the logs of the job
	@runai logs ${RUNAI_JOBNAME} -p ${RUNAI_PROJECT}

runai-delete:  ## RunAI - delete the job
	@runai delete job ${RUNAI_JOBNAME} -p ${RUNAI_PROJECT}

submit: runai-submit # hidden alias
logs: runai-logs # hidden alias
status: runai-status # hidden alias


HELP: # show this help
	@echo ENVIRONMENT VARIABLES
	@echo ========================
	@echo "DOCKER_IMAGE        ${DOCKER_IMAGE}"
	@echo "DOCKER_USER         ${DOCKER_USER}"
	@echo "DOCKER_NAME         ${DOCKER_NAME}"
	@echo "DOCKER_TAG          ${DOCKER_TAG}"
	@echo ------------------------
	@echo "DOCKER_PLATFORM     ${DOCKER_PLATFORM}"
	@echo "DOCKER_BUILD_FLAGS  ${DOCKER_BUILD_FLAGS}"
	@echo "DOCKER_ENTRYPOINT   ${DOCKER_ENTRYPOINT}"
	@echo ------------------------
	@echo "RUNAI_PROJECT       ${RUNAI_PROJECT}"
	@echo "RUNAI_JOBNAME       ${RUNAI_JOBNAME}"
	@echo ========================
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = "(:|## )"}; {printf "\033[36m%-15s\033[0m %s\n", $$2, $$4}'
