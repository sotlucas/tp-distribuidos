SHELL := /bin/bash
PWD := $(shell pwd)

docker-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./health_checker/Dockerfile -t "health_checker:latest" .
	docker build -f ./processors/filter/Dockerfile -t "filter:latest" .
	docker build -f ./processors/tres_escalas_o_mas/Dockerfile -t "tres_escalas_o_mas:latest" .
	docker build -f ./processors/dos_mas_rapidos/Dockerfile -t "dos_mas_rapidos:latest" .
	docker build -f ./processors/distancias/Dockerfile -t "distancias:latest" .
	docker build -f ./processors/media_general/Dockerfile -t "media_general:latest" .
	docker build -f ./processors/max_avg/Dockerfile -t "max_avg:latest" .
	docker build -f ./processors/tagger/Dockerfile -t "tagger:latest" .
	docker build -f ./processors/load_balancer/Dockerfile -t "load_balancer:latest" .
	docker build -f ./processors/joiner/Dockerfile -t "joiner:latest" .
	docker build -f ./processors/grouper/Dockerfile -t "grouper:latest" .
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
	docker compose -f docker-compose.yml stop -t 6
	docker compose -f docker-compose.yml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose.yml logs -f
.PHONY: docker-compose-logs

docker-compose-stop:
	docker compose -f docker-compose.yml stop -t 6
.PHONY: docker-compose-stop

test:
	python3 -m pytest -v
.PHONY: test

check:
	./tools/result_checker/check.sh
.PHONY: check