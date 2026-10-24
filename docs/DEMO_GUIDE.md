# Teacher demonstration (8–10 minutes)

Prepare the images and download well before presenting. Run tests, load the real sample, and measure benchmarks beforehand so the demo doesn't depend on a slow first download. Use only public demo strings. Start from the isolated demo store if you need repeatable resets; see README.

1. **0:00–1:00 — Scope and provenance.** Explain that this is a local prototype demonstrating an architecture for larger datasets. Show acquisition manifest, selected prefixes, checksum and actual row count under Data Pipeline. It is a subset of HIBP SHA-1 hashes, not plaintext or the full corpus. Do not claim billions processed.
2. **1:00–2:30 — Real Spark batch.** Click Process real sample. Show the job moving queued/running/completed, actual input/unique/invalid/duplicate metrics and local Spark mode. Explain Parquet intermediates and maximum-count deduplication. If rerunning after the sample was loaded, exact-store insertions can be zero even though Spark processes all rows again.
3. **2:30–3:30 — Data quality fixture.** Process cleaning fixture. Show its 21 input rows, 7 invalid, 5 unique valid hashes and 9 duplicate valid rows removed. Show manifest normalization count (typically 7). Discuss distinct rejection reasons and the explicit synthetic banner. Combined mode overlaps the main corpus; do not interpret casing variants as new compromised passwords.
4. **3:30–4:45 — Private checker.** Check `password`, whose prefix was included by acquisition. Show browser Network panel: one `/api/range/5BAA6` request, no body, no full hash, suffix or plaintext. Walk through local hash → prefix-only transfer → local suffix comparison. The result says found/not found **in loaded dataset**, never absolutely safe. Source counts can include labelled synthetic records.
5. **4:45–6:30 — Kafka and Spark.** Open Live Stream. Verify reachable Kafka and running Spark query. Click Start simulation. Copy its public string (`ClassDemoOnly-2026!`, or deterministic suffix if previously loaded). Within ten seconds check it and show not found. Return to Live Stream; show produced=1, an applied micro-batch, inserted=1, index version advancement and last update. Check the same string again: found. Explicitly say this is a synthetic event transported by real Kafka and cleaned by Spark, not a newly discovered real breach. A slow machine may exceed five seconds; wait for applied batch status.
6. **6:30–8:00 — Measured comparison.** Open Filter Comparison. Run or show a completed benchmark. Compare actual packed sizes, measured lookup throughput, FPR denominators, member misses and failures at available scale points. Explain different Bloom/Cuckoo FPR configurations and that total system memory includes database/runtime. Show controlled Cuckoo deletion; explain possible residual fingerprint match and why Bloom bits cannot be cleared safely.
7. **8:00–9:00 — Reliability and limits.** Explain SQLite authority, replay-safe batches and startup filter rebuilds. Distinguish local multicore Spark from a cluster. Mention corpus coverage and fixed-anonymity-set limitations. State any unverified runtime portions from VERIFICATION.md honestly.

Keep a terminal ready with `docker compose logs --tail=50 stream` and `docker compose ps`. If Kafka/Spark is unavailable, show the actual error and explicitly omit the live transport claim; direct API updates do not count as a successful Kafka demonstration.

## Reliable examples and interpreting repeated runs

The acquired 120 ranges cover only about 0.0114% of all possible five-character SHA-1 prefixes. `password` was deliberately covered via prefix `5BAA6`; other common breached passwords can be absent from this subset. This is a dataset-coverage limit, not evidence those passwords are safe. Use `public-synthetic-control-0` and `public-synthetic-control-1` as additional known-positive examples, explicitly labelled synthetic fixture controls.

Repeated preprocessing does not increase the authoritative total: the same hashes are already present. Fixture-only cleaning still reports 21 input, 14 valid before deduplication, 7 invalid, 7 normalized, 9 duplicates removed and 5 unique. Three of those five hashes overlap the main sample; two are synthetic controls. Combined processing reports 236,516 input, 7 invalid, 12 duplicates removed and 236,497 unique, for the current acquired sample.

For streaming, use two browser tabs so the first can remain on Live Stream while the second performs the initial check during the producer's ten-second delay. Each Start simulation emits exactly one synthetic event, then the producer stops normally. Keep the Spark terminal open. Current WSL restart command: `wsl -d BDC-Spark -- bash /mnt/d/BDC/scripts/start_wsl_stack.sh`.

A repeatable automated proof, using only a public generated string:

```powershell
wsl -d BDC-Spark -- bash -c 'source /mnt/d/BDC/scripts/wsl_env.sh; python scripts/check_stream_demo.py'
```

It checks absence, emits via the simulation producer, waits for the private range result to change, verifies total/index-version advancement, and saves `artifacts/metrics/latest-stream-proof.json`. It does not insert directly into SQLite or call the internal ingestion endpoint to bypass Kafka.
