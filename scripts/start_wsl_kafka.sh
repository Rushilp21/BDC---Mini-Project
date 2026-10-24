#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/wsl_env.sh"
kafka_root="$PROJECT_ROOT/.runtime/kafka_2.12-3.7.1"
config="$PROJECT_ROOT/.runtime/kafka-wsl.properties"
cat > "$config" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9095
listeners=PLAINTEXT://127.0.0.1:9094,CONTROLLER://127.0.0.1:9095
advertised.listeners=PLAINTEXT://localhost:9094
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=$PROJECT_ROOT/.runtime/kafka-wsl-data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
EOF
java -cp "$kafka_root/libs/*" kafka.tools.StorageTool format --ignore-formatted -t MkU3OEVBNTcwNTJENDM2Qk -c "$config"
exec java -Xmx512m -cp "$kafka_root/libs/*" kafka.Kafka "$config"
