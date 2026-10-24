import importlib
import pytest
from fastapi.testclient import TestClient

@pytest.fixture
def client(tmp_path,monkeypatch):
    monkeypatch.setenv('DATA_DIR',str(tmp_path))
    monkeypatch.setenv('SQLITE_PATH',str(tmp_path/'test-api.sqlite'))
    monkeypatch.setenv('ADMIN_TOKEN','test-secret')
    import backend.app.main as module
    module=importlib.reload(module)
    with TestClient(module.app) as c:
        yield c
    module.store.db.close()

def test_prefix_validation_and_admin(client):
    assert client.get('/api/range/abcde').status_code==422
    assert client.get('/api/range/ABCDE').json()['candidates']==[]
    assert client.post('/api/indexes/build').status_code==403
    assert client.get('/api/stats').json()['total_hashes']==0
    assert client.get('/api/benchmarks',headers={'X-Admin-Token':'test-secret'}).json()==[]

def test_internal_validation_and_replay(client):
    from backend.tests.test_store import row
    headers={'X-Admin-Token':'test-secret'}
    payload=dict(stream_id='api-test',batch_id=1,records=[row()],input_records=1,invalid_records=0)
    assert client.post('/api/internal/batches',json=payload,headers=headers).json()['inserted']==1
    assert client.post('/api/internal/batches',json=payload,headers=headers).json()['replayed']
    payload['records'][0]['count']=2
    assert client.post('/api/internal/batches',json=payload,headers=headers).status_code==409
    payload['records'][0]['hash']='broken'
    assert client.post('/api/internal/batches',json=payload,headers=headers).status_code==422

def test_upload_path_and_size_type(client):
    headers={'X-Admin-Token':'test-secret'}
    assert client.post('/api/datasets/import',files={'file':('evil.zip',b'bad')},headers=headers).status_code==422
    result=client.post('/api/datasets/import',files={'file':('../../test.csv',b'hash,count,source\n')},headers=headers)
    assert result.status_code==200
    assert '/' not in result.json()['dataset_id']
    assert client.post('/api/jobs/preprocess',json={'mode':'upload','dataset_id':'../../test.csv'},headers=headers).status_code==422


def test_background_job_lifecycle(client):
    import time
    headers={'X-Admin-Token':'test-secret'}
    r=client.post('/api/indexes/build',headers=headers)
    assert r.status_code==200
    identity=r.json()['job_id']
    for _ in range(50):
        state=client.get('/api/jobs/'+identity,headers=headers).json()
        if state['status'] in ('completed','failed'):break
        time.sleep(.02)
    assert state['status']=='completed'
    assert state['result']['ready']
