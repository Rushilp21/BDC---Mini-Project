import hashlib
import math
import pytest
from backend.app.filters import BloomFilter, CuckooFilter

def hashes(n):
    return [hashlib.sha1(f'public-test-{i}'.encode()).hexdigest().upper() for i in range(n)]

@pytest.mark.parametrize('cls',[BloomFilter,CuckooFilter])
def test_members_and_persistence(cls):
    f = cls(2000)
    values = hashes(1000)
    assert all(f.insert(h) for h in values)
    assert all(f.contains(h) for h in values)
    restored = cls.loads(f.dumps())
    assert all(restored.contains(h) for h in values)

def test_bloom_sizing_stability():
    b = BloomFilter(1000,.01)
    assert b.m == math.ceil(-1000*math.log(.01)/math.log(2)**2)
    assert b.positions('abc') == BloomFilter(1000).positions('abc')
    assert isinstance(b.bits,bytearray)

def test_cuckoo_relocation_failure_rollback():
    f = CuckooFilter(20,max_kicks=200)
    stored = []
    failed = False
    for h in hashes(300):
        if f.insert(h): stored.append(h)
        else:
            failed = True
            assert all(f.contains(v) for v in stored)
            break
    assert failed
    assert f.size == len(stored)

def test_cuckoo_known_deletion():
    values=hashes(100)
    f=CuckooFilter(1000)
    for h in values: assert f.insert(h)
    assert f.delete(values[0])
    assert all(f.contains(h) for h in values[1:])
