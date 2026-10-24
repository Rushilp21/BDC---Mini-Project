"""One-time offline migration from the Windows mount to WSL's Linux filesystem."""
import shutil
import sqlite3
from pathlib import Path
source=Path('data/breach.sqlite')
target=Path('/opt/bdc-state/breach.sqlite')
if target.exists():raise SystemExit('Target already exists; refusing to overwrite')
if Path(str(source)+'-wal').exists():raise SystemExit('Source WAL remains; close all database owners first')
target.parent.mkdir(parents=True,exist_ok=True)
shutil.copy2(source,target)
with sqlite3.connect(target) as db:
    assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
    print('Migrated exact hashes:',db.execute('SELECT COUNT(*) FROM hashes').fetchone()[0])
