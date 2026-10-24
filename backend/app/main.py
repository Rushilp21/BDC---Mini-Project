import gzip
import hashlib
import json
import os
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Header, HTTPException, UploadFile
from pydantic import BaseModel, Field, ConfigDict
from .store import Store, now
from .benchmark import benchmark

ROOT = Path(os.getenv('PROJECT_ROOT', Path(__file__).resolve().parents[2]))
DATA = Path(os.getenv('DATA_DIR', ROOT / 'data'))
DATA.mkdir(exist_ok=True, parents=True)
store = Store(Path(os.getenv('SQLITE_PATH', DATA / 'breach.sqlite')))
executor = ThreadPoolExecutor(max_workers=1)
producer_stop = threading.Event()
producer_state = dict(running=False, produced=0, error=None, demo=None)


@asynccontextmanager
async def lifespan(app):
    yield
    producer_stop.set()
    executor.shutdown(wait=False, cancel_futures=True)


app = FastAPI(title='Password Breach Intelligence', lifespan=lifespan)


def admin(x_admin_token: str = Header(default='')):
    import secrets
    expected = os.getenv('ADMIN_TOKEN', 'local-demo-admin')
    if not secrets.compare_digest(x_admin_token, expected):
        raise HTTPException(403, 'Local admin token required')


def job(kind, task):
    identity = uuid.uuid4().hex
    with store.lock, store.db:
        store.db.execute('INSERT INTO jobs VALUES(?,?,?,?,?,?)', (identity, kind, 'queued', None, None, now()))
    def work():
        try:
            with store.lock, store.db:
                store.db.execute("UPDATE jobs SET status='running' WHERE id=?", (identity,))
            result = task(identity)
            with store.lock, store.db:
                store.db.execute("UPDATE jobs SET status='completed',result=? WHERE id=?", (json.dumps(result), identity))
        except Exception as exc:
            with store.lock, store.db:
                store.db.execute("UPDATE jobs SET status='failed',error=? WHERE id=?", (str(exc), identity))
    executor.submit(work)
    return dict(job_id=identity)


@app.get('/api/health')
def health():
    return dict(status='ok', index_ready=store.ready)


@app.get('/api/stats')
def stats():
    result = store.stats()
    manifests = list((DATA / 'raw').glob('*.provenance.json'))
    result['provenance'] = [json.loads(p.read_text()) for p in manifests]
    return result


@app.get('/api/range/{prefix}')
def ranges(prefix: str):
    if not re.fullmatch('[0-9A-F]{5}', prefix):
        raise HTTPException(422, 'Expected five uppercase SHA-1 hexadecimal characters')
    return dict(candidates=store.candidates(prefix), coverage='Loaded subset only; no fixed anonymity-set guarantee')


@app.post('/api/datasets/import', dependencies=[Depends(admin)])
async def upload(file: UploadFile):
    name = file.filename or ''
    ext = ''.join(Path(name).suffixes).lower()
    if ext not in ('.txt', '.csv', '.json', '.jsonl', '.txt.gz', '.csv.gz', '.json.gz', '.jsonl.gz'):
        raise HTTPException(422, 'Use TXT, CSV, JSON, JSONL, optionally GZIP')
    folder = DATA / 'raw'
    folder.mkdir(exist_ok=True)
    target = folder / (uuid.uuid4().hex + ext)
    size = 0
    try:
        with target.open('wb') as out:
            while chunk := await file.read(1024*1024):
                size += len(chunk)
                if size > 100*1024*1024:
                    raise HTTPException(413, 'Upload limit is 100 MiB')
                out.write(chunk)
        if ext.endswith('.gz'):
            expanded = 0
            with gzip.open(target, 'rb') as inp:
                while chunk := inp.read(1024*1024):
                    expanded += len(chunk)
                    if expanded > 512*1024*1024:
                        raise HTTPException(413, 'Expanded GZIP limit is 512 MiB')
    except Exception:
        target.unlink(missing_ok=True)
        raise
    return dict(dataset_id=target.name, bytes=size, provenance='Uploaded data; provenance unverified')


class ProcessRequest(BaseModel):
    mode: str = 'fixture'
    dataset_id: str | None = None


@app.post('/api/jobs/preprocess', dependencies=[Depends(admin)])
def preprocess(body: ProcessRequest):
    if body.mode not in ('fixture', 'sample', 'combined', 'upload'):
        raise HTTPException(422, 'Unknown mode')
    paths = []
    if body.mode in ('sample', 'combined'):
        paths.append(DATA / 'raw' / 'hibp_sample.csv')
    if body.mode in ('fixture', 'combined'):
        paths.append(DATA / 'fixtures' / 'cleaning_test.csv')
    if body.mode == 'upload':
        if not body.dataset_id or Path(body.dataset_id).name != body.dataset_id:
            raise HTTPException(422, 'Invalid dataset ID')
        paths.append(DATA / 'raw' / body.dataset_id)
    if any(not p.is_file() for p in paths):
        raise HTTPException(422, 'Input missing; acquire sample or generate fixture first')
    def task(identity):
        from spark.batch_pipeline import run, session
        output = DATA / 'processed' / identity
        report = run(paths, output)
        frame = session().read.parquet(str(output))
        # Driver streams rows into bounded SQLite batches; Spark writes the clean Parquet.
        records, totals = [], dict(inserted=0, duplicates=0)
        for row in frame.toLocalIterator():
            records.append(row.asDict())
            if len(records) >= 5000:
                result = store.ingest(records)
                for k in totals: totals[k] += result[k]
                records.clear()
        if records:
            result = store.ingest(records)
            for k in totals: totals[k] += result[k]
        return dict(processing=report, ingestion=totals, parquet=str(output))
    return job('preprocess', task)


@app.get('/api/jobs/{identity}', dependencies=[Depends(admin)])
def get_job(identity: str):
    with store.reader() as db:
        row = db.execute('SELECT * FROM jobs WHERE id=?', (identity,)).fetchone()
        if not row: raise HTTPException(404, 'Job not found')
        result = dict(row)
    result['result'] = json.loads(result['result']) if result['result'] else None
    return result


@app.get('/api/jobs', dependencies=[Depends(admin)])
def jobs():
    with store.reader() as db:
        return [dict(r) for r in db.execute('SELECT * FROM jobs ORDER BY created_at DESC LIMIT 30')]


@app.post('/api/indexes/build', dependencies=[Depends(admin)])
def rebuild():
    return job('index', lambda _: store.rebuild())


@app.get('/api/indexes/status')
def index_status():
    with store.lock: return store.index_status()


class MembershipRequest(BaseModel):
    hash: str = Field(pattern='^[0-9A-F]{40}$')


@app.post('/api/indexes/check', dependencies=[Depends(admin)])
def membership(body: MembershipRequest):
    # Maintenance/benchmark path only; the private browser checker never calls this.
    with store.lock:
        return dict(ready=store.ready,
                    bloom=('Possibly present' if store.bloom.contains(body.hash) else 'Definitely absent') if store.ready else 'Unavailable',
                    cuckoo=('Possibly present' if store.cuckoo.contains(body.hash) else 'Definitely absent') if store.ready else 'Unavailable',
                    exact=bool(store.db.execute('SELECT 1 FROM hashes WHERE hash=?', (body.hash,)).fetchone()))


class Record(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    hash: str = Field(pattern='^[0-9A-F]{40}$')
    count: int = Field(ge=0, le=9223372036854775807)
    source: str = Field(min_length=1, max_length=500)


class Batch(BaseModel):
    model_config = ConfigDict(extra='forbid')
    stream_id: str = Field(min_length=1, max_length=100)
    batch_id: int = Field(ge=0)
    records: list[Record] = Field(max_length=10000)
    input_records: int = Field(ge=0)
    invalid_records: int = Field(ge=0)


@app.post('/api/internal/batches', dependencies=[Depends(admin)])
def ingest(body: Batch):
    if body.input_records != len(body.records) + body.invalid_records:
        raise HTTPException(422, 'Batch metric denominator mismatch')
    payload = body.model_dump()
    checksum = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    try:
        return store.ingest(payload['records'], dict(stream_id=body.stream_id, batch_id=body.batch_id,
                            checksum=checksum, metrics=dict(input_records=body.input_records, invalid_records=body.invalid_records)))
    except ValueError as exc:
        raise HTTPException(409, str(exc))


@app.post('/api/benchmarks/run', dependencies=[Depends(admin)])
def run_benchmark():
    def task(identity):
        with store.lock: values = store.hashes()
        points = sorted(set([min(10000, len(values)), min(100000, len(values)), len(values)]))
        result = dict(scale_runs=[benchmark(values[:n], authoritative=values) for n in points if n], measured_at=now())
        if not result['scale_runs']: raise ValueError('Load data before benchmarking')
        folder = ROOT / 'artifacts' / 'metrics'
        folder.mkdir(exist_ok=True, parents=True)
        (folder / f'benchmark-{identity}.json').write_text(json.dumps(result, indent=2))
        return result
    return job('benchmark', task)


@app.get('/api/benchmarks', dependencies=[Depends(admin)])
def benchmarks():
    with store.lock:
        return [json.loads(r[0]) for r in store.db.execute("SELECT result FROM jobs WHERE kind='benchmark' AND status='completed' ORDER BY created_at DESC LIMIT 5")]


@app.post('/api/simulation/start', dependencies=[Depends(admin)])
def start_simulation():
    if producer_state['running']: return producer_state
    with store.lock:
        i = 0
        while True:
            demo = 'ClassDemoOnly-2026!' + (f'-{i}' if i else '')
            h = hashlib.sha1(demo.encode()).hexdigest().upper()
            if not store.db.execute('SELECT 1 FROM hashes WHERE hash=?', (h,)).fetchone(): break
            i += 1
    producer_state.update(running=True, produced=0, error=None, demo=demo)
    producer_stop.clear()
    def produce():
        from kafka import KafkaProducer
        producer = None
        try:
            producer = KafkaProducer(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092'),
                        value_serializer=lambda v: json.dumps(v).encode(), api_version_auto_timeout_ms=5000,
                        request_timeout_ms=10000, max_block_ms=10000)
            # Delay lets the demonstrator check the chosen public string before its event.
            if not producer_stop.wait(10):
                event = dict(event_id=uuid.uuid4().hex, hash=h, count=1, source='synthetic-stream-demo', emitted_at=now())
                producer.send('breach-hashes', event).get(timeout=15)
                producer_state['produced'] += 1
        except Exception as exc:
            producer_state['error'] = str(exc)
        finally:
            if producer: producer.close(timeout=5)
            producer_state['running'] = False
    threading.Thread(target=produce, daemon=True).start()
    return producer_state


@app.post('/api/simulation/stop', dependencies=[Depends(admin)])
def stop_simulation():
    producer_stop.set()
    return producer_state


@app.get('/api/stream/status', dependencies=[Depends(admin)])
def stream_status():
    heartbeat = DATA / 'stream-status.json'
    stream = json.loads(heartbeat.read_text()) if heartbeat.exists() else dict(status='not_started')
    if stream.get('heartbeat_epoch') and time.time() - stream['heartbeat_epoch'] > 30:
        stream['status'] = 'stale_or_stopped'
    broker = False
    try:
        import socket
        host, port = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092').split(':')
        with socket.create_connection((host, int(port)), timeout=.5): broker = True
    except OSError: pass
    return dict(producer=producer_state, spark=stream, kafka_tcp_reachable=broker, backend='ok', **store.stats())
