"""Local diagnostic: no client-password information is read or printed."""
import sqlite3
from pathlib import Path
path=Path('data/breach.sqlite').resolve()
print('Database exists:',path.exists(), 'bytes:',path.stat().st_size)
for query_only in (False,True):
    try:
        c=sqlite3.connect(str(path),timeout=5)
        if query_only:c.execute('PRAGMA query_only=ON')
        print('query_only:',query_only,'count:',c.execute('SELECT COUNT(*) FROM hashes').fetchone()[0])
        c.close()
    except Exception as e: print(type(e).__name__,str(e))
