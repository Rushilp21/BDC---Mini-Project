"""Create/reset an isolated demo store. Never delete the authoritative corpus.

Stop backend and stream before reset. The explicit --stopped assertion is required.
"""
import argparse
import os
import shutil
import sqlite3
from pathlib import Path


def prepare(reset=False, stopped=False):
    root = Path(__file__).resolve().parents[1]
    demo = root / 'data' / 'demo'
    if reset and not stopped: raise ValueError('Stop services first and pass --stopped')
    demo.mkdir(exist_ok=True)
    marker = demo / '.isolated-demo'
    snapshot = demo / 'baseline.sqlite'
    target = demo / 'breach.sqlite'
    if reset:
        if not marker.exists() or not snapshot.exists(): raise ValueError('Initialize demo first')
        with sqlite3.connect(snapshot) as src, sqlite3.connect(target) as dst:
            src.backup(dst)
        checkpoint = (demo / 'checkpoints').resolve()
        if checkpoint.parent != demo.resolve(): raise ValueError('Unexpected checkpoint target')
        if checkpoint.exists(): shutil.rmtree(checkpoint)
        (demo / 'stream-status.json').unlink(missing_ok=True)
        print('Restored isolated demo snapshot. Original data/breach.sqlite unchanged.')
        return
    if marker.exists(): raise ValueError('Demo already initialized; use --reset --stopped')
    source = Path(os.getenv('SQLITE_PATH', root / 'data' / 'breach.sqlite'))
    if not source.exists(): raise ValueError('Load the main sample first')
    for folder in ('raw','fixtures','processed'):
        (demo / folder).mkdir(exist_ok=True)
    for filename in ('hibp_sample.csv','hibp_sample.provenance.json'):
        p = root / 'data/raw' / filename
        if p.exists(): shutil.copy2(p, demo / 'raw' / filename)
    for p in (root / 'data/fixtures').glob('*'): shutil.copy2(p, demo / 'fixtures' / p.name)
    with sqlite3.connect(source) as src, sqlite3.connect(snapshot) as dst: src.backup(dst)
    with sqlite3.connect(snapshot) as src, sqlite3.connect(target) as dst: src.backup(dst)
    marker.write_text('Isolated demo database; safe offline snapshot restore')
    print('Demo prepared. Set DATA_PATH=./data/demo before docker compose up.')


if __name__ == '__main__':
    p=argparse.ArgumentParser();p.add_argument('--reset',action='store_true');p.add_argument('--stopped',action='store_true')
    a=p.parse_args();prepare(a.reset,a.stopped)
