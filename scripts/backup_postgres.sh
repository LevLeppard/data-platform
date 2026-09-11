#!/usr/bin/env bash
set -e

BACKUP_DIR="$(dirname "$0")/../backups/postgres"
DATE=$(date +%Y%m%d_%H%M%S)
KEEP=3

mkdir -p "$BACKUP_DIR"

if ! docker ps --format '{{.Names}}' | grep -q '^dbt-postgres-eia$'; then
    echo "dbt-postgres-eia not running, skipping backup"
    exit 0
fi

echo "Backing up dbtdb..."
docker exec dbt-postgres-eia pg_dump -U dbtuser -d dbtdb -F c -f /tmp/backup.dump
docker cp dbt-postgres-eia:/tmp/backup.dump "$BACKUP_DIR/dbtdb_${DATE}.dump"
docker exec dbt-postgres-eia rm /tmp/backup.dump

# Оставить только последние 3 слепка
cd "$BACKUP_DIR"
ls -t *.dump 2>/dev/null | tail -n +$((KEEP + 1)) | xargs -r rm

echo "Backup saved: dbtdb_${DATE}.dump"