SHELL := /bin/bash
PWD := $(shell pwd)

docker-image:
	docker build -f ./uploader/Dockerfile -t "uploader:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./filter/Dockerfile -t "filter:latest" .
	docker build -f ./processors/tres_escalas_o_mas/Dockerfile -t "tres_escalas_o_mas:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f docker-compose.yml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose.yml stop -t 1
	docker compose -f docker-compose.yml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose.yml logs -f
.PHONY: docker-compose-logs