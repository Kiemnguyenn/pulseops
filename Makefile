.PHONY: up down restart log

up:
	docker-compose -f infra/docker-compose.yml up -d

down:
	docker-compose -f infra/docker-compose.yml down

restart: down up

log:
	docker-compose -f infra/docker-compose.yml logs -f
