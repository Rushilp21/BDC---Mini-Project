# Architecture and measurement semantics

## Components

The React browser hashes the exact entered password bytes using UTF-8 and Web Crypto SHA-1. Only `GET /api/range/ABCDE` is transmitted. SQLite's indexed `prefix` column enumerates candidate suffixes. Comparison occurs locally; the input is cleared after a successful check. No password/hash/suffix is stored in browser storage or analytics. API and Nginx access logs are disabled. Error messages do not echo client password input.

The complete corpus is never presumed loaded. Prefix sampling is not uniform sampling of passwords. Every source is visible. Five prefix characters do not guarantee a particular anonymity-set size. A match may refer to a labelled synthetic record; counts represent source observations, with maximum-per-hash deduplication.

FastAPI is the sole owner of the live packed indexes, in one worker. An RLock serializes mutations and administrative membership checks. Private prefix reads and job polling use independent read-only SQLite WAL snapshots, so they do not wait for mutable-filter publication. SQLite transactions commit exact rows, batch identity and index version together. Filter publication follows the commit while holding the lock. If an index update fails, exact records remain and `ready` becomes false; membership endpoints return unavailable. Prefix queries remain authoritative. Rebuilds size a replacement with headroom, retry Cuckoo insertion with larger capacity and publish only completed indexes. Startup always reconstructs from SQLite; stale snapshots are never served.

Batch jobs and benchmark jobs run in a single background executor, outside the async request loop. Jobs persist in SQLite. Interrupted jobs become failed on restart. This demo uses a single backend and a single Spark streaming process; do not launch competing stream consumers against the same checkpoint.

## Batch semantics

CSV/JSON permissive parser corrupt rows receive `malformed_row`. TXT lines require exactly one colon. Trim **surrounding** hash whitespace, uppercase hexadecimal, then require exactly 40 hex characters. Do not strip internal characters. Count is required, decimal non-negative integer within signed SQLite int64 range. Duplicates take maximum count, retaining sorted unique sources.

Each parsed row receives exactly one rejection reason in order: malformed row, missing hash, invalid hash, invalid count. Thus `input = valid_before_dedup + invalid`, `valid_before_dedup = unique + duplicate_rows_removed`, and rejection totals equal invalid. Normalization counts changed hash strings across **all parsed rows**, including invalid whitespace-only strings; it is not a disjoint category of validity. Files that cannot be read fail the job instead of being counted as rejected parsed rows. A successful report has `read_failures=0`; an unsuccessful read is visible as a failed job without manufactured row denominators. Durations include Spark startup/read/clean/write/report calculations, ending before database ingestion. Throughput uses total parsed input divided by that duration.

The normalized DataFrame is persisted while metrics and output are computed. The aggregated clean frame is persisted for unique count and Parquet output. Parquet rows stream through the driver into 5K-row SQLite update chunks. An interrupted historical import can leave a partial exact corpus; rerun the idempotent max-count merge to complete it. It does not advertise a completed processing job until ingestion finishes.

Fixture manifests describe fixture-only processing independently. If its three selected hashes overlap the main sample, combined deduplication removes three more rows than the fixture-only count (assuming the sample has unique valid hashes). Processing the same file repeatedly does not create additional exact records.

## Filters

Bloom uses `ceil(-n ln(p)/ln(2)^2)` bits, `max(1, round(m/n ln(2)))` positions, and a packed bytearray. Positions use deterministic BLAKE2b double hashing, never Python's salted hash. Target p is 1% **at provisioned capacity**, not a claim of observed FPR at lower occupancy. Production capacity is at least 1,024 or twice current unique rows. Bloom has no deletion.

Cuckoo uses power-of-two bucket counts, four packed uint16 slots per bucket and 10-bit fingerprints in 1..1023, reserving zero for empty. Candidate buckets use XOR, making alternate relocation reversible. Maximum 500 kicks; a failed attempt rolls back every overwritten slot in reverse order. Backend retains exact records and rebuilds a larger filter. Cuckoo load above 90% or Bloom capacity overflow triggers rebuilding. Approximate Cuckoo candidate-slot collision probability is up to 8/1023 at full occupancy; actual observed FPR depends on load and data.

Both implement serialization/reload; local pickle snapshots must never be loaded from untrusted uploads. Live startup uses authoritative reconstruction instead. Deletion demonstration operates on a separate 100-item synthetic filter only. Deleting a known member can still yield a positive because fingerprints collide. Deleting an arbitrary probable member could remove another record's fingerprint and is not exposed as a public corpus operation.

## Streaming

Synthetic producer → KRaft Kafka `breach-hashes` → `readStream.format('kafka')` → explicit JSON schema → shared Spark normalization/validation → `foreachBatch` → token-protected internal ingestion endpoint. This is a simulated arrival over real transport, not a live HIBP breach feed.

Each checkpoint directory has a persisted UUID `stream-id`; micro-batch IDs are scoped to it. Ordered valid records and parsed-row metrics form a canonical SHA-256 payload checksum. A replay with identical identity/checksum returns the saved application result; a mismatched replay fails with HTTP 409. Counts use max, never repeated addition. Incoming hashes already stored are never inserted again into Cuckoo. A lost HTTP acknowledgement is safe because Spark retries the same checkpoint batch. HTTP failure terminates the query visibly; restart replays from the checkpoint. Status heartbeat becomes stale after 30 seconds. TCP reachability is labelled as such and is not proof of successful Kafka delivery; completed micro-batches provide transport evidence.

Streaming metrics include input, invalid, accepted/received, newly inserted, exact duplicates and updated metadata. Spark accepts only labelled synthetic events with an event ID for this demo. `maxOffsetsPerTrigger=5000` bounds a driver-collected micro-batch. Five-second triggers are approximate, not a latency guarantee.

## Benchmarks

Both indexes build from the same sorted unique clean hashes at the same headroom factor. Test every member for misses. Generate deterministic non-members and verify absence against the entire authoritative snapshot. One warmup, five timed workloads, `perf_counter_ns`, median batch time/query. False positives are divided by verified absent queries; zero observations is not zero probability. Build time includes insertion but excludes filter allocation. Packed and serialized size exclude SQLite, runtime and Spark memory. Bloom's configured target and Cuckoo's fingerprint configuration differ and are disclosed. Hardware, Python, local Spark mode and actual scale points are recorded; no multi-node performance claims or invented worker-utilization charts.

## Compatibility references checked 2026-10-24

* [Spark 3.5.7 overview](https://spark.apache.org/docs/3.5.7/): Python 3.8+, Java 8/11/17, Scala 2.12/2.13. Image pins Python 3.11.9 and Temurin Java 17.0.12+7.
* [Spark Kafka integration](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html): matching `spark-sql-kafka-0-10_2.12:3.5.7`; supports Kafka brokers 0.10+. Do not mix connector/Spark/Scala versions.
* [Official Kafka image](https://hub.docker.com/r/apache/kafka): pins broker 3.7.1, single combined broker/controller KRaft node, no ZooKeeper. This is a demo topology, not production fault tolerance.
* Frontend versions are exact in package.json and transitive versions in package-lock.json. Python direct dependencies are pinned in backend/requirements.txt; transitive environment versions are recorded in verification outputs where available.
* The verified WSL fallback used Ubuntu Python 3.10.12 and OpenJDK 17.0.20.1. Its extracted Apache Spark 3.5.7 Python modules match the pinned PySpark version. See LOCAL_WSL.md; Docker image execution remains separately unverified while the desktop engine is unavailable.
