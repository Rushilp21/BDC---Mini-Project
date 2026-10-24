"""Exercise completed API workflows and save measured evidence; needs running stack."""
import json
import os
import time
from pathlib import Path
import httpx


def main():
    folder=Path('artifacts/metrics');folder.mkdir(parents=True,exist_ok=True)
    with httpx.Client(base_url=os.getenv('BACKEND_URL','http://localhost:8000'),headers={'X-Admin-Token':os.getenv('ADMIN_TOKEN','local-demo-admin')},timeout=120) as c:
        def job(path,body=None):
            r=c.post(path,json=body);r.raise_for_status();identity=r.json()['job_id']
            deadline=time.monotonic()+600
            while time.monotonic()<deadline:
                r=c.get('/api/jobs/'+identity);r.raise_for_status();state=r.json()
                if state['status']=='failed':raise RuntimeError(state['error'])
                if state['status']=='completed':return state['result']
                time.sleep(2)
            raise TimeoutError('Job did not complete')
        evidence={}
        for mode in ('sample','fixture','combined'):
            print('Processing '+mode,flush=True)
            evidence[mode]=job('/api/jobs/preprocess',dict(mode=mode))
            (folder/'demo-verification.json').write_text(json.dumps(evidence,indent=2))
        manifest=json.loads(Path('data/fixtures/cleaning_test.manifest.json').read_text())
        for key in ('input_records','invalid_records','normalized_rows','unique_hashes','duplicates_removed','rejection_reasons'):
            assert evidence['fixture']['processing'][key]==manifest[key],key
        assert evidence['combined']['processing']['duplicates_removed']==manifest['duplicates_removed']+len(manifest['selected_hashes'])
        print('Benchmarking actual loaded hashes',flush=True)
        evidence['benchmark']=job('/api/benchmarks/run')
        evidence['stats']=c.get('/api/stats').json()
        evidence['stream']=c.get('/api/stream/status').json()
        (folder/'demo-verification.json').write_text(json.dumps(evidence,indent=2))
        print('Saved artifacts/metrics/demo-verification.json',flush=True)


if __name__=='__main__':main()
