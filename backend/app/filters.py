"""Packed persistent filters; all positions use deterministic BLAKE2b."""
import hashlib
import math
import pickle
import random
from array import array


def digest(value: str, salt=b"") -> int:
    return int.from_bytes(hashlib.blake2b(value.encode(), key=salt, digest_size=16).digest(), "big")


class BloomFilter:
    def __init__(self, capacity, error_rate=0.01):
        self.capacity = max(1, capacity)
        self.m = math.ceil(-self.capacity * math.log(error_rate) / math.log(2) ** 2)
        self.k = max(1, round(self.m / self.capacity * math.log(2)))
        self.bits = bytearray((self.m + 7) // 8)
        self.size = 0

    def positions(self, value):
        a, b = digest(value), digest(value, b"positions") | 1
        return [(a + i * b) % self.m for i in range(self.k)]

    def insert(self, value):
        for p in self.positions(value):
            self.bits[p // 8] |= 1 << (p % 8)
        self.size += 1
        return True

    def contains(self, value):
        return all(self.bits[p // 8] & (1 << (p % 8)) for p in self.positions(value))

    def dumps(self):
        return pickle.dumps(self, protocol=5)

    @staticmethod
    def loads(blob):
        # Only load snapshots created locally; never accept uploaded pickle files.
        return pickle.loads(blob)


class CuckooFilter:
    """4-slot buckets, 10-bit fingerprints, <=500 kicks with transactional rollback.

    At most ~8/1024 candidate-slot collision probability; deletion requires an
    authoritative known member. Packed uint16 slots deliberately reserve 0.
    """
    def __init__(self, capacity, max_kicks=500):
        self.bucket_size = 4
        self.buckets = 1 << max(1, math.ceil(math.log2(max(2, capacity / 3.2))))
        self.slots = array("H", [0]) * (self.buckets * self.bucket_size)
        self.capacity = self.buckets * self.bucket_size
        self.size = 0
        self.max_kicks = max_kicks

    def locations(self, value):
        h = digest(value)
        fp = 1 + (h % 1023)
        i = (h >> 32) & (self.buckets - 1)
        return fp, i, self.alt(i, fp)

    def alt(self, bucket, fp):
        return bucket ^ (digest(str(fp), b"alternate") & (self.buckets - 1))

    def put(self, bucket, fp):
        for offset in range(4):
            p = bucket * 4 + offset
            if self.slots[p] == 0:
                self.slots[p] = fp
                return True
        return False

    def contains(self, value):
        fp, a, b = self.locations(value)
        return any(fp == self.slots[i * 4 + j] for i in (a, b) for j in range(4))

    def insert(self, value):
        fp, a, b = self.locations(value)
        if self.put(a, fp) or self.put(b, fp):
            self.size += 1
            return True
        rng = random.Random(digest(value, b"kick"))
        bucket = rng.choice((a, b))
        changes = []
        for _ in range(self.max_kicks):
            p = bucket * 4 + rng.randrange(4)
            changes.append((p, self.slots[p]))
            self.slots[p], fp = fp, self.slots[p]
            bucket = self.alt(bucket, fp)
            if self.put(bucket, fp):
                self.size += 1
                return True
        for p, old in reversed(changes):
            self.slots[p] = old
        return False

    def delete(self, known_member):
        fp, a, b = self.locations(known_member)
        for i in (a, b):
            for j in range(4):
                p = i * 4 + j
                if self.slots[p] == fp:
                    self.slots[p] = 0
                    self.size -= 1
                    return True
        return False

    def dumps(self):
        return pickle.dumps(self, protocol=5)

    @staticmethod
    def loads(blob):
        return pickle.loads(blob)
