import hashlib
import platform
import os
import statistics
import time
from .filters import BloomFilter, CuckooFilter


def benchmark(values, query_count=10000, authoritative=None):
    exact = set(values)
    if not exact:
        raise ValueError('Load a clean dataset before benchmarking')
    authority = set(authoritative) if authoritative is not None else exact
    absent, i = [], 0
    while len(absent) < query_count:
        h = hashlib.sha1(f'public-benchmark-absent-{i}'.encode()).hexdigest().upper()
        if h not in authority:
            absent.append(h)
        i += 1
    members = sorted(exact)
    workload = members[:query_count] + absent
    results = []
    for cls in (BloomFilter, CuckooFilter):
        f = cls(max(1, len(members) * 2))
        t = time.perf_counter_ns()
        inserted = sum(bool(f.insert(h)) for h in members)
        build = time.perf_counter_ns() - t
        misses = sum(not f.contains(h) for h in members)
        fp = sum(f.contains(h) for h in absent)
        for h in workload:  # warmup
            f.contains(h)
        samples = []
        for _ in range(5):
            t = time.perf_counter_ns()
            for h in workload:
                f.contains(h)
            samples.append((time.perf_counter_ns() - t) / len(workload))
        ns = statistics.median(samples)
        results.append(dict(filter=cls.__name__, dataset_size=len(members), build_ms=build/1e6,
                            successful_inserts=inserted, failed_inserts=len(members)-inserted,
                            packed_bytes=len(f.bits) if isinstance(f, BloomFilter) else len(f.slots)*f.slots.itemsize,
                            serialized_bytes=len(f.dumps()), median_lookup_ns=ns, queries_per_second=1e9/ns,
                            false_positives=fp, nonmember_tests=len(absent), false_positive_rate=fp/len(absent),
                            member_misses=misses, capacity=f.capacity, load=f.size/f.capacity))
    # Delete only from a separate controlled index; assert other members remain.
    fixture = [hashlib.sha1(f'deletion-control-{i}'.encode()).hexdigest().upper() for i in range(100)]
    c = CuckooFilter(256)
    for h in fixture:
        assert c.insert(h)
    deleted = c.delete(fixture[0])
    return dict(results=results, deletion=dict(deleted=deleted, still_matches=c.contains(fixture[0]),
                other_member_misses=sum(not c.contains(h) for h in fixture[1:]),
                note='Fingerprint collisions may leave a deleted hash matching. Authoritative corpus unchanged.'),
                protocol='Same members and verified absent hashes; 1 warmup, 5 timed batches, median per-query time. Bloom target 1%; Cuckoo 10-bit fingerprints (different FPR).',
                machine=dict(platform=platform.platform(), processor=platform.processor(), logical_cpus=os.cpu_count(), python=platform.python_version()),
                verified_against_hashes=len(authority),
                memory_scope='Filter packed bytes and serialization only; excludes SQLite, Python runtime and Spark')
