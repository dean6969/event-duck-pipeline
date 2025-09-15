#!/usr/bin/env bash
set -e

echo "🛑 Stopping and removing containers, networks, volumes, and local images for this project..."

# Chỉ xóa resource liên quan đến docker-compose.yml hiện tại
docker compose down -v --rmi local --remove-orphans

echo "✅ Project resources removed (containers, networks, volumes, images built locally)."
