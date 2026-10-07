.PHONY: up down status backup

up:
	export $$(grep -v '^#' airflow/.env | xargs) && cd airflow && astro dev start
	@NETWORK_NAME=$$(docker inspect $$(docker ps --filter "name=scheduler" --format "{{.Names}}" | head -n1) --format '{{range $$k, $$v := .NetworkSettings.Networks}}{{$$k}}{{end}}'); \
	echo "Detected network: $$NETWORK_NAME"; \
	ASTRO_NETWORK_NAME=$$NETWORK_NAME docker compose up -d; \
	cd metabase && ASTRO_NETWORK_NAME=$$NETWORK_NAME docker compose --env-file ../.env up -d

backup:
	@bash scripts/backup_postgres.sh

down: backup
	docker compose down 2>/dev/null || true
	cd metabase && docker compose --env-file ../.env down 2>/dev/null || true
	cd airflow && astro dev stop

status:
	docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
	@ls -la backups/postgres/ 2>/dev/null || echo "No backups yet"