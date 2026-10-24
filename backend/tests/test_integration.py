"""Opt-in real network smoke: import -> Spark -> indexes -> Kafka -> Spark -> prefix."""
import hashlib
import os
import time
import pytest
import httpx

@pytest.mark.integration
@pytest.mark.skipif(os.getenv('RUN_INTEGRATION')!='1',reason='Set RUN_INTEGRATION=1 with Compose running')
def test_real_kafka_spark_smoke():
    with httpx.Client(base_url=os.getenv('BACKEND_URL','http://localhost:8000'),headers={'X-Admin-Token':os.getenv('ADMIN_TOKEN','local-demo-admin')},timeout=30) as c:
        def wait_job(identity):
            deadline=time.monotonic()+180
            while time.monotonic()<deadline:
                r=c.get('/api/jobs/'+identity);r.raise_for_status();j=r.json()
                if j['status']=='failed':pytest.fail(j['error'])
                if j['status']=='completed':return j
                time.sleep(2)
            pytest.fail('Spark job timed out')
        h=hashlib.sha1(b'public-import-smoke').hexdigest().upper()
        r=c.post('/api/datasets/import',files={'file':('smoke.csv',f'hash,count,source\n{h},1,synthetic-e2e-test\n'.encode())});r.raise_for_status()
        j=c.post('/api/jobs/preprocess',json=dict(mode='upload',dataset_id=r.json()['dataset_id']));j.raise_for_status()
        wait_job(j.json()['job_id'])
        assert any(r['suffix']==h[5:] for r in c.get('/api/range/'+h[:5]).json()['candidates'])
        assert c.get('/api/indexes/status').json()['ready']
        r=c.post('/api/simulation/start');r.raise_for_status();demo=r.json()['demo']
        h=hashlib.sha1(demo.encode()).hexdigest().upper()
        assert all(r['suffix']!=h[5:] for r in c.get('/api/range/'+h[:5]).json()['candidates'])
        deadline=time.monotonic()+120
        while time.monotonic()<deadline:
            if any(r['suffix']==h[5:] for r in c.get('/api/range/'+h[:5]).json()['candidates']):break
            time.sleep(2)
        else:pytest.fail('Kafka -> Spark -> backend update timed out')
        assert c.get('/api/indexes/status').json()['ready']


@pytest.mark.integration
@pytest.mark.skipif(os.getenv('RUN_INTEGRATION')!='1',reason='Set RUN_INTEGRATION=1 with real Kafka and Spark')
def test_invalid_kafka_events_and_exact_deduplication():
    import json
    import uuid
    from kafka import KafkaProducer
    with httpx.Client(base_url=os.getenv('BACKEND_URL','http://localhost:8000'),headers={'X-Admin-Token':os.getenv('ADMIN_TOKEN','local-demo-admin')},timeout=30) as c:
        h=hashlib.sha1(b'public-import-smoke').hexdigest().upper()
        # Ensure the existing public fixture is loaded via the historical pipeline.
        assert any(r['suffix']==h[5:] for r in c.get('/api/range/'+h[:5]).json()['candidates'])
        before=c.get('/api/stats').json()
        prior={(b['stream_id'],b['batch_id']) for b in before['batches']}
        producer=KafkaProducer(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP','localhost:9092'),value_serializer=lambda v:json.dumps(v).encode())
        try:
            for value in (h,h,'INVALID'):
                producer.send('breach-hashes',dict(event_id=uuid.uuid4().hex,hash=value,count=1,source='synthetic-stream-demo',emitted_at='2026-10-24T00:00:00Z')).get(timeout=15)
        finally:producer.close()
        deadline=time.monotonic()+120
        while time.monotonic()<deadline:
            after=c.get('/api/stats').json()
            batches=[json.loads(b['metrics']) for b in after['batches'] if (b['stream_id'],b['batch_id']) not in prior]
            if sum(b['input_records'] for b in batches)>=3:
                assert sum(b['invalid_records'] for b in batches)==1
                assert sum(b['duplicates'] for b in batches)==2
                assert after['total_hashes']==before['total_hashes']
                break
            time.sleep(2)
        else:pytest.fail('Invalid-event micro-batch did not arrive')
