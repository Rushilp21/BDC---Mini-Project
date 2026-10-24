#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/wsl_env.sh"
mkdir -p .runtime
if ! python -c 'import socket; socket.create_connection(("127.0.0.1",9094),1).close()' 2>/dev/null; then
  nohup bash scripts/start_wsl_kafka.sh > .runtime/kafka-wsl.log 2>&1 &
  echo $! > .runtime/kafka-wsl.pid
fi
for i in $(seq 1 40); do
  if python -c 'import socket; socket.create_connection(("127.0.0.1",9094),1).close()' 2>/dev/null; then break; fi
  sleep 1
done
java -cp '.runtime/kafka_2.12-3.7.1/libs/*' org.apache.kafka.tools.TopicCommand --bootstrap-server localhost:9094 --create --if-not-exists --topic breach-hashes --partitions 1 --replication-factor 1
if ! python -c 'import urllib.request; urllib.request.urlopen("http://127.0.0.1:8000/api/health",timeout=2)' 2>/dev/null; then
  nohup uvicorn backend.app.main:app --host 127.0.0.1 --port 8000 --workers 1 --no-access-log > .runtime/backend.log 2>&1 &
  echo $! > .runtime/backend.pid
fi
for i in $(seq 1 60); do
  if python -c 'import urllib.request; urllib.request.urlopen("http://127.0.0.1:8000/api/health",timeout=2)' 2>/dev/null; then break; fi
  sleep 1
done
echo 'API: http://127.0.0.1:8000; Kafka (inside WSL): localhost:9094'
echo 'Start frontend from Windows: cd frontend; npm run dev'
echo 'Consumer startup/status: tail -f .runtime/stream.log'
if ! python -c 'import json,time; from pathlib import Path; d=json.loads(Path("data/stream-status.json").read_text()); assert d["status"]=="running" and time.time()-d["heartbeat_epoch"]<30' 2>/dev/null; then
  echo 'Starting Spark in the foreground. Keep this terminal open; use a second terminal for the frontend.'
  echo $$ > .runtime/stream.pid
  exec spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 spark/streaming_pipeline.py > .runtime/stream.log 2>&1
fi
