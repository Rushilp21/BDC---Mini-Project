#!/usr/bin/env bash
# Source from the repository root in a Linux/WSL shell; only the optional fallback uses this.
export PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export SPARK_HOME="$PROJECT_ROOT/.runtime/spark-3.5.7-bin-hadoop3"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PYSPARK_PYTHON=/opt/bdc-venv/bin/python
export PYTHONPATH="$PROJECT_ROOT:$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip"
export PATH="/opt/bdc-venv/bin:$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH"
export SPARK_LOCAL_IP=127.0.0.1
export KAFKA_BOOTSTRAP=localhost:9094
export BACKEND_URL=http://127.0.0.1:8000
export ADMIN_TOKEN="${ADMIN_TOKEN:-local-demo-admin}"
export SQLITE_PATH="${SQLITE_PATH:-/opt/bdc-state/breach.sqlite}"
export LANG=C.UTF-8
cd "$PROJECT_ROOT"
