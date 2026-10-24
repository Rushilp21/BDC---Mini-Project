# Implementation verification — 2026-10-24

This workspace was empty initially. All application components were implemented here. No multi-node or billion-record claims are made.

## Completed and measured

* Downloaded **236,495 real HIBP SHA-1 records** from 120 bounded prefix ranges. Full hashes reconstructed from prefix + suffix. [Provenance](data-provenance.json) records acquisition date, range selection and file checksum. Raw data is excluded from Git.
* Ran actual Spark 3.5.7 DataFrame parsing, normalization, validation, max-count deduplication and Parquet writes. [Processing results](processing-results.json): real sample 236,495 valid/unique; fixture 21 input, 7 invalid, 7 normalized, 9 duplicates removed, 5 unique; combined 236,516 input, 7 invalid, 12 duplicates removed, 236,497 unique. Fixture and combined assertions passed in `scripts/verify_demo.py`.
* A warm real-sample cleaning pass measured **14.595 seconds / 16,203.9 input records per second**, excluding SQLite ingestion. The first cold pass took 89.786 seconds. These are local execution measurements, not a general throughput promise.
* SQLite exact storage, indexed prefix enumeration, both live packed filters, maximum-count/source merging, replay checksum enforcement and startup index reconstruction are working. Interrupted jobs remain visible as failed; they were not rewritten as successes.
* Browser interaction checked the public string `password`: found in the real loaded sample, prefix `5BAA6`, 1,978 candidates, occurrence count 52,372,427 at acquisition time. The password field cleared after success. Automated frontend tests verify that only `/api/range/5BAA6` and GET/cache options are sent; complete hash/suffix/password remain local.
* Real Kafka 3.7.1 KRaft broker and matching Spark/Scala connector executed. Synthetic events traversed Kafka → Structured Streaming → internal API → exact store and live indexes; a previously absent public demo string became present. The consumer recovered from an initial backend restart using its persistent checkpoint.
* [Saved benchmark output](benchmark-results.json) covers actual 10K, 100K and **236,499** loaded unique hashes (real sample plus labelled test records). Both filters had **0 member misses and 0 failed inserts** at all measured scales. At the largest point: Bloom packed bytes 566,715, build 1,799.75 ms, median lookup 5,878.97 ns; Cuckoo packed bytes 2,097,152, build 896.01 ms, median lookup 4,275.91 ns. False positives: **4/10,000** vs **12/10,000** verified non-members. Different configurations and loads are disclosed. This is not equal-FPR tuning, and it does not measure total process memory.
* Controlled deletion used a separate 100-item synthetic Cuckoo index: deletion succeeded, deleted item did not match in this measured run, and other members had zero misses. A residual positive remains possible with colliding fingerprints.
* Separate demo snapshot creation and offline reset were executed successfully. The authoritative active store was not reset.

## Actual test execution

* Backend/Spark regression suite: **22 passed**, 2 live tests deselected; 155.33 seconds. Includes fixture manifests, all formats, counts, overlap, packed filters, relocation rollback, serialization, capacity rebuilding, replay/restart, prefix validation, upload restrictions, background jobs and independent prefix reads.
* Component/API rerun after SQLite placement fix: **10 passed**; 34.61 seconds. API tests use isolated temporary SQLite paths.
* Frontend: **2 passed**; TypeScript and Vite production build passed. Final npm audit: **0 advisories**. Build reports a non-fatal approximately 513 KB JavaScript chunk warning.
* Live Kafka/Spark integration: **2 passed**, 22 regression tests deselected; 189.11 seconds. Verified import → Spark → indexes → private range match → Kafka arrival → Spark publication → changed result. A later batch contained 3 events: 2 accepted exact duplicates, 1 rejected invalid event, 0 new exact rows. Applied batch metrics matched these denominators.
* Compose configuration validation and Python compilation passed.

Raw JUnit reports and full API demonstration evidence are under `artifacts/metrics/`. Benchmark and processing summaries are copied into this docs directory so they can be reviewed without committing raw datasets or runtime databases.

## Runtime and remaining limits

Verified environment: independent Ubuntu 22.04 WSL2 (`BDC-Spark`), Python 3.10.12, OpenJDK 17.0.20.1, Spark 3.5.7 `local[2]`, Kafka 3.7.1, 8 visible logical CPUs. Spark's Kafka connector is `spark-sql-kafka-0-10_2.12:3.5.7`. Frontend ran under Node 22.14.0. [Exact prepared-runtime restart instructions](LOCAL_WSL.md) are provided.

**Docker images were not executed in this environment**: Docker Desktop crashed during inference-manager socket startup before exposing its engine. Compose configuration passed validation and official Kafka image configuration was checked, but image build/runtime verification remains outstanding. Actual Spark/Kafka transport was exercised in the independent WSL runtime instead.

Native Windows Spark hit Hadoop's missing winutils support at Parquet write. A later cross-platform SQLite WAL locking issue was resolved by moving the active WSL database to `/opt/bdc-state/breach.sqlite` on the Linux filesystem. The Windows-mounted troubleshooting copy is preserved. Avoid simultaneously accessing Linux SQLite through Windows processes.

Only sample-sized local workloads were measured. A million-record run, equal-FPR filter tuning, multi-node scalability, production authentication/TLS, distributed job scheduling, and external exposure hardening remain outside this prototype's verified scope. The database/filter set is in memory or on one local machine. Never infer absolute password safety from absence in a subset.
