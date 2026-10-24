import hashlib
import pytest
from backend.app.store import Store
from backend.app.benchmark import benchmark

def row(text='public-demo'):
    return dict(hash=hashlib.sha1(text.encode()).hexdigest().upper(),count=1,source='synthetic-test')

def test_ingestion_replay_restart_and_prefix(tmp_path):
    path=tmp_path/'test.sqlite'
    s=Store(path)
    r=row()
    batch=dict(stream_id='test-stream',batch_id=1,checksum='a')
    assert s.ingest([r],batch)['inserted']==1
    assert s.ingest([r],batch)['replayed']
    assert s.ingest([r])['inserted']==0
    assert s.bloom.size==s.cuckoo.size==1
    assert s.candidates(r['hash'][:5])==[dict(suffix=r['hash'][5:],count=1)]
    with pytest.raises(ValueError): s.ingest([r],dict(batch,checksum='different'))
    s.db.close()
    restarted=Store(path)
    assert restarted.bloom.contains(r['hash'])
    assert restarted.cuckoo.contains(r['hash'])
    assert restarted.index_status()['version']==1

def test_store_count_and_source_union(tmp_path):
    s=Store(tmp_path/'test.sqlite')
    r=row()
    s.ingest([r,dict(r,count=10,source='synthetic-other')])
    assert s.candidates(r['hash'][:5])[0]['count']==10
    assert s.stats()['total_hashes']==1
    assert s.bloom.size==1

def test_capacity_rebuild(tmp_path):
    s=Store(tmp_path/'test.sqlite')
    s.ingest([row(f'headroom-{i}') for i in range(2000)])
    assert s.ready
    assert all(s.cuckoo.contains(h) and s.bloom.contains(h) for h in s.hashes())

def test_benchmark_actual_denominator():
    result=benchmark([row(str(i))['hash'] for i in range(100)],query_count=1000)
    for r in result['results']:
        assert r['nonmember_tests']==1000
        assert r['false_positive_rate']==r['false_positives']/1000
        assert r['member_misses']==0
        assert r['successful_inserts']==100
        assert r['median_lookup_ns']>0
    with pytest.raises(ValueError): benchmark([])


def test_scale_nonmembers_checked_against_full_corpus():
    outside_scale=hashlib.sha1(b'public-benchmark-absent-0').hexdigest().upper()
    member=row()['hash']
    result=benchmark([member],query_count=10,authoritative=[member,outside_scale])
    assert result['verified_against_hashes']==2


def test_prefix_reads_dont_wait_for_filter_publication(tmp_path):
    import threading
    s=Store(tmp_path/'read.sqlite')
    r=row();s.ingest([r])
    done=threading.Event()
    def read():
        assert s.candidates(r['hash'][:5])[0]['count']==1
        done.set()
    with s.lock:
        t=threading.Thread(target=read);t.start()
        assert done.wait(2), 'Private range lookup blocked behind mutable-filter lock'
    t.join()
