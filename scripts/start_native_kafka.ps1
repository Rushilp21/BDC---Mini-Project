$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot
$javaPath = Join-Path $projectRoot '.runtime/java17/jdk-17.0.12+7/bin/java.exe'
$kafkaRoot = Join-Path $projectRoot '.runtime/kafka_2.12-3.7.1'
if (!(Test-Path $javaPath) -or !(Test-Path $kafkaRoot)) { throw 'Native fallback needs Java 17.0.12+7 and official Kafka 3.7.1 extracted under .runtime. Prefer Docker for fresh setups.' }
$logPath = (Join-Path $projectRoot '.runtime/kafka-data').Replace('\','/')
$configPath = Join-Path $projectRoot '.runtime/kafka.properties'
@"
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=$logPath
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
"@ | Set-Content -LiteralPath $configPath
& $javaPath -cp "$kafkaRoot/libs/*" kafka.tools.StorageTool format --ignore-formatted -t MkU3OEVBNTcwNTJENDM2Qk -c $configPath
if ($LASTEXITCODE -ne 0) { throw 'Kafka storage format failed' }
& $javaPath '-Xmx512m' -cp "$kafkaRoot/libs/*" kafka.Kafka $configPath
