#!/bin/bash

set -e

COMPOSE_FILE="hadoop-project/docker-compose.yml"

echo "Uploading logs to HDFS through master container..."

docker compose -f "$COMPOSE_FILE" exec -T master hdfs dfs -mkdir -p /raw_logs/generated_logs
docker compose -f "$COMPOSE_FILE" exec -T master hdfs dfs -mkdir -p /loghub_windows2k_data
docker compose -f "$COMPOSE_FILE" exec -T master hdfs dfs -mkdir -p /logs/windows

docker compose -f "$COMPOSE_FILE" exec -T master sh -lc 'if ls /data/raw_logs/generated_logs/* >/dev/null 2>&1; then hdfs dfs -put -f /data/raw_logs/generated_logs/* /raw_logs/generated_logs/; fi'
docker compose -f "$COMPOSE_FILE" exec -T master sh -lc 'if [ -f /data/raw_logs/Windows_2k.log_structured.csv ]; then hdfs dfs -put -f /data/raw_logs/Windows_2k.log_structured.csv /loghub_windows2k_data/; fi'
docker compose -f "$COMPOSE_FILE" exec -T master sh -lc 'if [ -f /data/raw_logs/Windows.log ]; then hdfs dfs -put -f /data/raw_logs/Windows.log /logs/windows/; fi'

echo "Upload complete."

docker compose -f "$COMPOSE_FILE" exec -T master hdfs dfs -ls /raw_logs
docker compose -f "$COMPOSE_FILE" exec -T master hdfs dfs -ls /loghub_windows2k_data
docker compose -f "$COMPOSE_FILE" exec -T master hdfs dfs -ls /logs/windows
