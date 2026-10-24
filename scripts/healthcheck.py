"""Read-only demonstration readiness check."""
import json
import os
import httpx
with httpx.Client(base_url=os.getenv('BACKEND_URL','http://localhost:8000'),headers={'X-Admin-Token':os.getenv('ADMIN_TOKEN','local-demo-admin')},timeout=15) as c:
    health=c.get('/api/health');health.raise_for_status()
    status=c.get('/api/stream/status');status.raise_for_status()
    s=status.json()
    report=dict(api=health.json(),loaded_hashes=s['total_hashes'],kafka_tcp=s['kafka_tcp_reachable'],
                spark=s['spark']['status'],last_batch=s['spark'].get('latest_batch_id'),indexes=s['indexes'])
    print(json.dumps(report,indent=2))
    if not (report['api']['index_ready'] and report['kafka_tcp'] and report['spark']=='running'):
        raise SystemExit('Not ready: inspect backend/stream logs')
