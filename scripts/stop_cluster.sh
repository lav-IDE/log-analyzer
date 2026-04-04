#!/bin/bash

set -e

COMPOSE_FILE="hadoop-project/docker-compose.yml"

echo "Stopping Docker Hadoop cluster..."

docker compose -f "$COMPOSE_FILE" down

echo "Cluster stopped."
