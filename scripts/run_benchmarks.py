import json
import os
import time
import httpx
if __name__ == '__main__':
    client = httpx.Client(base_url=os.getenv('BACKEND_URL', 'http://localhost:8000'),
                           headers={'X-Admin-Token': os.getenv('ADMIN_TOKEN', 'local-demo-admin')}, timeout=30)
    r = client.post('/api/benchmarks/run'); r.raise_for_status()
    identity = r.json()['job_id']
    while True:
        r = client.get('/api/jobs/' + identity); r.raise_for_status()
        state = r.json()
        if state['status'] in ('completed', 'failed'):
            print(json.dumps(state, indent=2)); break
        time.sleep(2)
