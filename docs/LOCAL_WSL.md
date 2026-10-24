# Verified WSL fallback on this Windows machine

Docker Desktop crashed before its engine started; native Windows Spark could read/clean but could not write Parquet without Hadoop Windows support. An independent Ubuntu 22.04 WSL2 distribution named `BDC-Spark` was therefore created under `D:\BDC\.runtime\wsl`. It does not change Docker's distribution or volumes. The complete application runs with actual Kafka and Spark inside this Linux runtime. Java 17, the Python virtual environment and the **active SQLite database `/opt/bdc-state/breach.sqlite`** are inside its Linux filesystem. Source data, Parquet and reports remain under D:\BDC. The earlier `data/breach.sqlite` is a preserved troubleshooting copy; it is not the active WSL database. Never open a live Linux SQLite database with a Windows SQLite process; cross-platform WAL locking on mounted drives caused read failures during verification.

## Restart the already prepared environment

From PowerShell at `D:\BDC`:

```powershell
wsl -d BDC-Spark -- bash /mnt/d/BDC/scripts/start_wsl_stack.sh
```

Keep that terminal open: it owns the foreground Spark consumer. In a **second PowerShell terminal** at `D:\BDC`:

```powershell
cd frontend
npm run dev
```

Use the URL Vite prints. The normal URL is `http://127.0.0.1:5173`; this implementation session's inspected dashboard is on `http://127.0.0.1:5174` because another development server used 5173. API is port 8000. WSL Kafka uses 9094 to avoid a temporary native Windows test broker on 9092. The environment script sets these consistently; do not mix them with Compose's listener addresses.

Stop processes started by the helper using the PID files under `.runtime` from inside the same WSL distribution, or stop each foreground terminal. Keep the WSL distribution running during the demo. `wsl --terminate BDC-Spark` stops only this isolated runtime; in-flight Spark jobs will be marked interrupted on API restart, and stream checkpoint replay recovers acknowledged batches. It does not delete the runtime, corpus or checkpoints.

## Reproduce the fallback from a fresh checkout (PowerShell)

Prefer Docker when its engine works. If using this independent WSL path, WSL2 must already be enabled. Downloads are official Ubuntu/Apache distributions. First prepare source data as in README, then:

```powershell
New-Item -ItemType Directory -Force .runtime/wsl | Out-Null
$ProgressPreference='SilentlyContinue'
Invoke-WebRequest 'https://cdimage.ubuntu.com/ubuntu-base/releases/22.04/release/ubuntu-base-22.04.5-base-amd64.tar.gz' -OutFile .runtime/ubuntu-base.tar.gz
Invoke-WebRequest 'https://archive.apache.org/dist/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3.tgz' -OutFile .runtime/spark-3.5.7-bin-hadoop3.tgz
Invoke-WebRequest 'https://archive.apache.org/dist/kafka/3.7.1/kafka_2.12-3.7.1.tgz' -OutFile .runtime/kafka.tgz
tar -xzf .runtime/spark-3.5.7-bin-hadoop3.tgz -C .runtime
tar -xzf .runtime/kafka.tgz -C .runtime
wsl --import BDC-Spark D:\BDC\.runtime\wsl D:\BDC\.runtime\ubuntu-base.tar.gz --version 2
wsl -d BDC-Spark -- bash -c 'apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y python3 python3-venv python3-pip openjdk-17-jre-headless ca-certificates'
wsl -d BDC-Spark -- bash /mnt/d/BDC/scripts/setup_wsl_runtime.sh
wsl -d BDC-Spark -- bash /mnt/d/BDC/scripts/start_wsl_stack.sh
```

Adjust both Windows and `/mnt/...` paths if checking out elsewhere. The optional setup script uses a PyPI mirror for small dependencies after primary PyPI downloads stalled during this session. Spark itself comes from Apache, with its Python modules supplied through `SPARK_HOME`/`PYTHONPATH`, rather than downloading another 317 MB PySpark package. Docker uses the pinned PyPI package normally. Review official Apache `.sha512` checksums for binary distributions when reproducing a download; the mirrored PySpark source used during troubleshooting matched PyPI SHA-256 `80e36514e0c5c126d35a26adf4f405cd4a69de96a8ea8a3c1e9a65fce2fb4eaa`.

## Verification commands in the prepared distribution

```powershell
wsl -d BDC-Spark -- bash -c 'source /mnt/d/BDC/scripts/wsl_env.sh; python -m pytest -m "not integration" -q'
wsl -d BDC-Spark -- bash -c 'source /mnt/d/BDC/scripts/wsl_env.sh; RUN_INTEGRATION=1 python -m pytest -m integration -q'
wsl -d BDC-Spark -- bash -c 'source /mnt/d/BDC/scripts/wsl_env.sh; python scripts/verify_demo.py'
```

`verify_demo.py` processes the real sample, fixture and combined corpus; compares fixture-only results to its manifest; checks combined overlap; runs actual scale benchmarks; and saves a JSON report. Wait for it to finish before taking measurements or restarting services. Local WSL filesystem/mounted-drive latency can make the first pass slower than a native Linux disk.
