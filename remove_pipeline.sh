#!/usr/bin/env bash
set -e

echo "🛑 Stopping and removing containers, networks, volumes, and images for this project..."

# Stop and remove all docker conatainer and volumes của docker-compose.yml
docker compose down -v --remove-orphans

# Delete all images (event-duck-pipeline)
PROJECT_NAME=$(basename "$PWD")
docker images --format "{{.Repository}}:{{.Tag}}" | grep "$PROJECT_NAME" | xargs -r docker rmi -f

# remove dangling & unused images
docker image prune -af

echo "✅ Project resources fully removed (containers, networks, volumes, and related images)."
