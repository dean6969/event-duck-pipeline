#!/usr/bin/env bash
set -e

echo "🛑 Stopping and removing containers, networks, volumes, and images for this project..."

# Dừng và xóa toàn bộ resource của docker-compose.yml
docker compose down -v --remove-orphans

# Xóa tất cả images có prefix theo tên project (event-duck-pipeline)
PROJECT_NAME=$(basename "$PWD")
docker images --format "{{.Repository}}:{{.Tag}}" | grep "$PROJECT_NAME" | xargs -r docker rmi -f

# Xóa luôn dangling & unused images
docker image prune -af

echo "✅ Project resources fully removed (containers, networks, volumes, and related images)."
