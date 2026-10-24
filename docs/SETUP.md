# Large-Scale Password Breach Intelligence and Privacy-Preserving Detection

A local Big Data Computing prototype: real HIBP hash acquisition → PySpark DataFrame cleaning → Parquet → SQLite and packed Bloom/Cuckoo indexes → browser-local SHA-1 prefix checking. Kafka carries **synthetic** arrivals through Spark Structured Streaming. This is an experimental comparison, not a claim of novel research or billion-record processing.

## Fresh checkout: recommended Docker path

Requirements: Docker Engine + Compose v2 (Docker Desktop with Linux containers and WSL2 on Windows), internet during first build/acquisition, Python 3.10+ for the two dependency-free preparation scripts. Allocate roughly 6 GB RAM to Docker. No host Java, Spark, Kafka or .NET installation is needed for this path.

Run from the repository root, in this order (commands work in PowerShell, bash and zsh except the environment syntax below):

```sh
python scripts/acquire_sample.py --ranges 120
python scripts/create_cleaning_fixture.py
docker compose build
docker compose up -d
docker compose ps
docker compose logs --tail=50 stream
```

Open [the dashboard](http://localhost:5173). The development admin token is `local-demo-admin`. To change it, copy `.env.example` to `.env`, edit `ADMIN_TOKEN`, recreate services, and type that token in the dashboard's sidebar. It stays in React memory. Bindings are loopback-only; this is a local teaching application, not an internet deployment. The private checker needs no admin token. All maintenance and full-hash inspection endpoints require it. Use one FastAPI worker.

In **Data Pipeline**, click **Process real sample**. Wait for its job to complete. This actually runs Spark and ingests the resulting Parquet. Then check the public demo password `password` (the acquisition deliberately includes prefix `5BAA6`). Benchmark from **Filter Comparison** after data processing completes. Nothing is prepopulated with fabricated measurements.

Acquire a small smoke sample with `--ranges 2`; 120 ranges currently yields approximately 100K–300K rows depending on the corpus. The script refuses more than 1,000 ranges. It reconstructs all 40 SHA-1 characters and records prefixes, acquisition time, record count, source and SHA-256 **file checksum**. Password queries and indexes consistently use **SHA-1**. No plaintext recovery is performed. Dataset files are Git-ignored.

If download fails, run the fixture generator and process the fixture to exercise synthetic mode. This does **not** satisfy real-data acquisition; the UI labels it synthetic. Do not describe fixture records as verified additional breaches.

## Importing other files

Use the dashboard file picker (100 MiB compressed upload / 512 MiB decompressed GZIP limit). Uploaded filenames are replaced with generated IDs. Then click **Process uploaded file**. Supported formats:

```text
# TXT: no header, full hashes only; do not include this comment
5BAA61E4C9B93F3F0682250B6CF8331B7EE68FD8:42
```

```csv
hash,count,source
5BAA61E4C9B93F3F0682250B6CF8331B7EE68FD8,42,my-documented-source
```

JSON is an array of objects with `hash`, `count`, `source`; JSONL has one object per line. GZIP is supported for each. Missing sources become `imported-unverified`; TXT sources are unverified. Uploaded provenance is caller supplied and is not independently certified. Never upload suffix-only range files: use the acquisition script, which reconstructs prefix + suffix. Dashboard processing merges into the loaded exact store; it does not wipe previously loaded records. Fixture-only metrics refer to that processing job, while the top summary refers to all stored hashes.

The official [PwnedPasswordsDownloader README](https://github.com/HaveIBeenPwned/PwnedPasswordsDownloader) was inspected on 2026-10-24. It describes a .NET 10 installation tool and whole-corpus downloads (not the dataset itself); `--single` joins all ranges and `--ntlm` changes algorithm. Our bounded Python range sampler avoids those whole-corpus commands. The [official range API](https://haveibeenpwned.com/API/v3#SearchingPwnedPasswordsByRange) accepts five SHA-1 prefix characters and returns suffix:count records.

## Tests and measurements

```sh
docker compose exec backend python -m pytest -m "not integration" -q
docker compose exec -e RUN_INTEGRATION=1 backend python -m pytest -m integration -q
docker compose exec backend python scripts/run_benchmarks.py
```

The image includes source tests. Frontend tests/build use a host Node 22.14+ installation:

```sh
cd frontend
npm ci
npm test
npm run build
```

The opt-in integration test imports a public synthetic full hash, runs real Spark cleaning, checks prefix retrieval, emits a real Kafka event, and waits for Spark to publish the changed result. It requires the complete Compose stack. It never silently substitutes direct ingestion for Kafka. Benchmark JSON is saved under `artifacts/metrics/`, and results persist in SQLite jobs. See [verification status](VERIFICATION.md) for what actually ran in this implementation environment.

## Host development (optional)

Use Python 3.10 or 3.11, Java **17**, Node 22.14+, and a separately running Kafka broker. Spark is configured as `local[2]` (one machine, two local worker threads, not a multi-node cluster).

PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r backend/requirements.txt
$env:JAVA_HOME='C:\path\to\jdk-17'
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
$env:PYSPARK_PYTHON=(Resolve-Path .venv/Scripts/python.exe).Path
$env:SPARK_LOCAL_IP='127.0.0.1'
.\.venv\Scripts\python -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8000 --workers 1 --no-access-log
```

Linux/macOS:

```sh
python3 -m venv .venv
.venv/bin/pip install -r backend/requirements.txt
export JAVA_HOME=/path/to/jdk-17
export PATH="$JAVA_HOME/bin:$PATH"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1
.venv/bin/uvicorn backend.app.main:app --host 127.0.0.1 --port 8000 --workers 1 --no-access-log
```

In a second terminal: `cd frontend`, `npm ci`, `npm run dev`. Vite proxies `/api` to port 8000. The Docker frontend uses Nginx's same-origin proxy. `localhost`/HTTPS is required for Web Crypto. On native Windows, Hadoop's filesystem layer may require matching winutils; prefer Linux containers/WSL2 for Spark writes.

Launch host streaming with the virtual environment active and the repository on `PYTHONPATH`:

```sh
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 spark/streaming_pipeline.py
```

Set `ADMIN_TOKEN` consistently in backend and streaming processes. `KAFKA_BOOTSTRAP` defaults to `localhost:9092`; `BACKEND_URL` to `http://localhost:8000`. In containers they are `kafka:29092` and `http://backend:8000`.

## Isolated repeatable demonstration and reset

After loading the main corpus, stop services and prepare an independent demo database:

```sh
docker compose stop
python scripts/demo_workspace.py
```

Set `DATA_PATH=./data/demo` (`$env:DATA_PATH='./data/demo'` in PowerShell, `export DATA_PATH=./data/demo` in bash), then run `docker compose up -d --force-recreate`. The original corpus database stays outside this mount. Start simulation to choose an absent deterministic public string. A ten-second delay permits its initial private lookup. After one synthetic event is acknowledged, the producer ends; the Spark consumer continues.

To repeat: `docker compose stop`, then `python scripts/demo_workspace.py --reset --stopped`, then `docker compose up -d`. The script restores only the isolated demo snapshot and checkpoint, with path checks. Kafka retains history, so the consumer replays old events after checkpoint reset. Exact deduplication makes this safe; the producer chooses the next absent deterministic string if a prior demo was replayed. Do not reset the main corpus or delete Kafka volumes for visual effect.

More detail: [architecture](ARCHITECTURE.md), [8–10 minute demo](DEMO_GUIDE.md), [troubleshooting](TROUBLESHOOTING.md), [prepared local WSL runtime and exact restart commands](LOCAL_WSL.md).
