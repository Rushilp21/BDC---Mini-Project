import json
import os
import time
import uuid
from pathlib import Path
import httpx
from pyspark.sql import functions as F, types as T
from spark.batch_pipeline import session, normalize


def main():
    data = Path(os.getenv('DATA_DIR', 'data'))
    checkpoint = data / 'checkpoints' / 'breach-stream'
    checkpoint.mkdir(parents=True, exist_ok=True)
    identity = checkpoint / 'stream-id'
    if not identity.exists(): identity.write_text(uuid.uuid4().hex)
    stream_id = identity.read_text().strip()
    status_path = data / 'stream-status.json'
    state = dict(status='starting', stream_id=stream_id, latest_batch_id=None, error=None)
    def status(**updates):
        state.update(updates, heartbeat_epoch=time.time())
        temp = status_path.with_suffix('.tmp')
        temp.write_text(json.dumps(state))
        temp.replace(status_path)
    try:
        status()
        spark = session('Breach Kafka Structured Streaming')
        spark.sparkContext.setLogLevel('WARN')
        schema = T.StructType([T.StructField(k, T.StringType()) for k in ('event_id', 'hash', 'count', 'source', 'emitted_at', '_corrupt_record')])
        events = (spark.readStream.format('kafka')
                  .option('kafka.bootstrap.servers', os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092'))
                  .option('subscribe', 'breach-hashes').option('startingOffsets', 'earliest')
                  .option('maxOffsetsPerTrigger', 5000).load())
        parsed = events.select(F.from_json(F.col('value').cast('string'), schema).alias('event')).select('event.*')
        cleaned = normalize(parsed)
        def apply_batch(frame, batch_id):
            frame.persist()
            try:
                total = frame.count()
                valid = frame.filter("reason IS NULL AND event_id IS NOT NULL AND emitted_at IS NOT NULL AND source = 'synthetic-stream-demo'")
                records = [dict(hash=r.hash, count=int(r.count_text), source=r.source) for r in valid.orderBy('hash', 'event_id').collect()]
                payload = dict(stream_id=stream_id, batch_id=batch_id, records=records,
                               input_records=total, invalid_records=total-len(records))
                # Failure stops the query; checkpoint replays this batch after restart.
                response = httpx.post(os.getenv('BACKEND_URL', 'http://localhost:8000')+'/api/internal/batches',
                                      headers={'X-Admin-Token': os.getenv('ADMIN_TOKEN', 'local-demo-admin')}, json=payload, timeout=120)
                response.raise_for_status()
                status(status='running', latest_batch_id=batch_id, last_batch=response.json(), last_success=time.time(), error=None)
            finally:
                frame.unpersist()
        query = (cleaned.writeStream.foreachBatch(apply_batch).option('checkpointLocation', str(checkpoint))
                 .trigger(processingTime='5 seconds').start())
        while query.isActive:
            status(status='running')
            query.awaitTermination(5)
        if query.exception(): raise query.exception()
        status(status='stopped')
    except Exception as exc:
        status(status='failed', error=str(exc))
        raise


if __name__ == '__main__': main()
