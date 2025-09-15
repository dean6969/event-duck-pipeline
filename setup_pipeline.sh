#!/usr/bin/env bash
set -e

# create .env and airflow uid
if [ ! -f .env ]; then
  echo "AIRFLOW_UID=$(id -u)" > .env
  echo ".env file created with AIRFLOW_UID=$(id -u)"
else
  if ! grep -q "AIRFLOW_UID" .env; then
    echo "AIRFLOW_UID=$(id -u)" >> .env
    echo "AIRFLOW_UID added to existing .env file"
  fi
fi

echo " Building Docker images..."
docker compose build

echo "Starting containers in detached mode..."
docker compose up -d

echo "Pipeline containers are up and running!"

URL="http://127.0.0.1:8080"
if command -v xdg-open &> /dev/null; then
    xdg-open "$URL"
elif command -v open &> /dev/null; then
    open "$URL"
elif command -v start &> /dev/null; then
    start "$URL"
else
    echo "🌐 Vui lòng mở thủ công: $URL"
fi
