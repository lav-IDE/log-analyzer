#!/bin/bash

set -e

COMPOSE_FILE="hadoop-project/docker-compose.yml"

echo "Starting 3-node Hadoop cluster with Docker Compose..."

docker compose -f "$COMPOSE_FILE" up -d --build

echo ""
echo "Cluster containers:"
docker compose -f "$COMPOSE_FILE" ps

echo ""
echo "HDFS UI: http://localhost:9870"
echo "YARN UI: http://localhost:8088"
