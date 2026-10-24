"""Prove one synthetic Kafka -> Spark update using prefix-only lookups.

Uses only a generated public demo string, never a personal password.
Run with the same BACKEND_URL/ADMIN_TOKEN settings as scripts/healthcheck.py.
"""
import hashlib
import json
import os
import time
from pathlib import Path
import httpx


def main():
    with httpx.Client(base_url=os.getenv('BACKEND_URL', 'http://localhost:8000'),
                      headers={'X-Admin-Token': os.getenv('ADMIN_TOKEN', 'local-demo-admin')}, timeout=20) as c:
        before = c.get('/api/stats'); before.raise_for_status(); before = before.json()
        response = c.post('/api/simulation/start'); response.raise_for_status()
        demo = response.json()['demo']
        value = hashlib.sha1(demo.encode()).hexdigest().upper()
        def found():
            response = c.get('/api/range/' + value[:5]); response.raise_for_status()
            return any(r['suffix'] == value[5:] for r in response.json()['candidates'])
        initially_found = found()
        print(f'Public synthetic string: {demo}', flush=True)
        print(f'Before Kafka arrival: found={initially_found}', flush=True)
        assert not initially_found, 'Demo string was already present before its delayed event'
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if found(): break
            time.sleep(2)
        else: raise TimeoutError('No Kafka/Spark update within 120 seconds; inspect stream status')
        after = c.get('/api/stats'); after.raise_for_status(); after = after.json()
        stream = c.get('/api/stream/status'); stream.raise_for_status(); stream = stream.json()
        assert after['total_hashes'] > before['total_hashes']
        assert after['indexes']['version'] > before['indexes']['version']
        assert after['indexes']['ready']
        report = dict(public_synthetic_string=demo, found_before=False, found_after=True,
                      total_before=before['total_hashes'], total_after=after['total_hashes'],
                      version_before=before['indexes']['version'], version_after=after['indexes']['version'],
                      kafka_tcp_reachable=stream['kafka_tcp_reachable'], spark_status=stream['spark']['status'],
                      last_batch=stream['spark'].get('latest_batch_id'), produced=stream['producer']['produced'])
        output = Path('artifacts/metrics/latest-stream-proof.json')
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2))
        print(json.dumps(report, indent=2))


if __name__ == '__main__': main()
