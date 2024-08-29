# default variables
DOCKER_NAME ?= era5-downloader
DOCKER_TAG ?= latest

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
.DEFAULT_GOAL := help
.PHONY: help


build:  ## Docker: build the docker image from ./Dockerfile
	@docker build --platform=${DOCKER_PLATFORM} -t ${DOCKER_IMAGE} .

run:  ## Docker: run the docker container from ./Dockerfile
	@docker run \
		--rm \
		--interactive \
		--tty \
		${DOCKER_IMAGE} $(ARGS)

bash:  ## Docker: run the docker container from ./Dockerfile with bash
	@docker run --rm -it --entrypoint /bin/bash ${DOCKER_IMAGE}

push:  ## Docker: push the docker image to dockerhub
	@echo "Pushing ${DOCKER_IMAGE} to dockerhub: https://hub.docker.com/repository/docker/${DOCKER_IMAGE}/"
	@docker push ${DOCKER_IMAGE}

login:  ## RunAI: login to runai
	@runai login

submit:  ## RunAI: submit the job to runai - also mounts the s3 bucket
	@runai submit ${RUNAI_JOBNAME} \
		--cpu ${CPUS} \
		--memory ${RAM} \
		-i ${DOCKER_IMAGE} \
		--interactive
	@rm -f .ssh-id-copied

ports:  ## RunAI: expose the ports of the job
	@if [ -f .port-forward.pid ] && kill -0 $$(cat .port-forward.pid) 2>/dev/null; then \
		echo "Port forwarding is already running with PID $$(cat .port-forward.pid)"; \
	else \
		echo Forwarding ports; \
		nohup runai port-forward ${JOBNAME} --port ${PORT_LOCAL_SSH}:${PORT_VM_SSH} --port ${PORT_LOCAL_NB}:${PORT_VM_NB} > /dev/null 2>&1 & echo "$$!" > .port-forward.pid; \
		sleep 2; \
	fi
	@echo "   SSH:      ${PORT_LOCAL_SSH}"
	@echo "   Notebook: ${PORT_LOCAL_NB}"

status:  # RunAI: get the status of the job
	runai describe job ${RUNAI_JOBNAME} -p ${RUNAI_PROJECT}

ssh-copy:  ports  ## copy the ssh key to localhost
	@if [ -f .ssh-id-copied ]; then \
			echo "SSH key already copied"; \
	else \
	 	echo "Attempting to copy ssh-key to localhost (password='password')"; \
		ssh-copy-id -i ~/.ssh/id_rsa.pub -p ${PORT_LOCAL_SSH} ${USER}@localhost; \
		echo "" > ".ssh-id-copied"; \
	fi

ssh:  ssh-copy  ## ssh into a localhost ssh server
	@ssh ${USER}@localhost -p ${PORT_LOCAL_SSH}

help:  ## show this help
	@echo ENVIRONMENT VARIABLES
	@echo ========================
	@echo "DOCKER_IMAGE     ${DOCKER_IMAGE}"
	@echo "DOCKER_PLATFORM  ${DOCKER_PLATFORM}"
	@echo "RUNAI_PROJECT    ${RUNAI_PROJECT}"
	@echo "RUNAI_JOBNAME    ${RUNAI_JOBNAME}"
	@echo ========================
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
