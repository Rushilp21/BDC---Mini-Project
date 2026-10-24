import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from .filters import BloomFilter, CuckooFilter


def now():
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = str(path)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(str(path), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS hashes(hash TEXT PRIMARY KEY,prefix TEXT NOT NULL,suffix TEXT NOT NULL,
                occurrence_count INTEGER NOT NULL,source TEXT NOT NULL,created_at TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS prefix_idx ON hashes(prefix);
            CREATE TABLE IF NOT EXISTS processed_batches(stream_id TEXT,batch_id INTEGER,checksum TEXT,
                applied_at TEXT,record_count INTEGER,metrics TEXT,PRIMARY KEY(stream_id,batch_id));
            CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,kind TEXT,status TEXT,result TEXT,error TEXT,created_at TEXT);
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT);
            INSERT OR IGNORE INTO meta VALUES('version','0');
        ''')
        self.db.execute("UPDATE jobs SET status='failed',error='Interrupted by backend restart' WHERE status IN ('queued','running')")
        self.db.commit()
        self.bloom = self.cuckoo = None
        self.ready = False
        self._stats_cache = None
        self.rebuild()

    def hashes(self):
        return [r[0] for r in self.db.execute("SELECT hash FROM hashes ORDER BY hash")]

    def rebuild(self):
        with self.lock:
            self.ready = False
            values = self.hashes()
            capacity = max(1024, len(values) * 2)
            for _ in range(5):
                b, c = BloomFilter(capacity), CuckooFilter(capacity)
                success = True
                for h in values:
                    b.insert(h)
                    if not c.insert(h):
                        success = False
                        break
                if success:
                    self.bloom, self.cuckoo, self.ready = b, c, True
                    return self.index_status()
                capacity *= 2
            raise RuntimeError("Cuckoo rebuild failed; exact store retained; filters unavailable")

    def index_status(self):
        return dict(ready=self.ready, version=int(self.db.execute("SELECT value FROM meta WHERE key='version'").fetchone()[0]),
                    bloom_capacity=self.bloom.capacity if self.bloom else None,
                    bloom_load=self.bloom.size / self.bloom.capacity if self.bloom else None,
                    cuckoo_load=self.cuckoo.size / self.cuckoo.capacity if self.cuckoo else None,
                    warning="Rebuild recommended near Bloom capacity or Cuckoo 90% load")

    def ingest(self, records, batch=None):
        with self.lock:
            if batch:
                old = self.db.execute("SELECT * FROM processed_batches WHERE stream_id=? AND batch_id=?", (batch['stream_id'], batch['batch_id'])).fetchone()
                if old:
                    if old['checksum'] != batch['checksum']:
                        raise ValueError("Batch identity reused with different payload")
                    return dict(replayed=True, **json.loads(old['metrics']))
            new = []
            changed = 0
            self.ready = False
            try:
                with self.db:
                    # Prefetch in portable SQLite-sized chunks instead of doing one
                    # filesystem-backed SELECT for every imported row.
                    requested = list(dict.fromkeys(r['hash'] for r in records))
                    known = {}
                    for offset in range(0, len(requested), 900):
                        chunk = requested[offset:offset+900]
                        placeholders = ','.join('?' for _ in chunk)
                        known.update({r['hash']: (r['occurrence_count'], r['source']) for r in self.db.execute(
                            f'SELECT hash,occurrence_count,source FROM hashes WHERE hash IN ({placeholders})', chunk)})
                    inserts, updates = {}, {}
                    for r in records:
                        h, count, source = r['hash'], int(r['count']), r['source']
                        old = known.get(h)
                        if old:
                            sources = '|'.join(sorted(set(old[1].split('|')) | set(source.split('|'))))
                            maximum = max(count, old[0])
                            if maximum != old[0] or sources != old[1]:
                                known[h] = (maximum, sources)
                                if h in inserts:
                                    inserts[h] = (h, h[:5], h[5:], maximum, sources, inserts[h][-1])
                                else:
                                    updates[h] = (maximum, sources, h)
                                changed += 1
                        else:
                            known[h] = (count, source)
                            inserts[h] = (h, h[:5], h[5:], count, source, now())
                            new.append(h)
                    self.db.executemany('INSERT INTO hashes VALUES(?,?,?,?,?,?)', inserts.values())
                    self.db.executemany('UPDATE hashes SET occurrence_count=?,source=? WHERE hash=?', updates.values())
                    metrics = dict(received=len(records), inserted=len(new), duplicates=len(records)-len(new), updated=changed)
                    if new or changed:
                        self.db.execute("UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='version'")
                    if batch:
                        metrics.update(batch.get('metrics', {}))
                        self.db.execute("INSERT INTO processed_batches VALUES(?,?,?,?,?,?)", (batch['stream_id'], batch['batch_id'], batch['checksum'], now(), len(records), json.dumps(metrics)))
                # Exact storage is committed first. A failed filter update cannot lose exact data.
                self._stats_cache = None
                if self.bloom.size + len(new) > self.bloom.capacity or (self.cuckoo.size + len(new)) / self.cuckoo.capacity > .9:
                    self.rebuild()
                else:
                    for h in new:
                        self.bloom.insert(h)
                        if not self.cuckoo.insert(h):
                            self.rebuild()
                            break
                    self.ready = True
                return dict(replayed=False, **metrics)
            except Exception:
                self.ready = False
                raise

    def candidates(self, prefix):
        # Prefix reads use an independent WAL snapshot and never inspect mutable
        # filters, so a bulk index publication need not block the private checker.
        with self.reader() as db:
            return [dict(suffix=r['suffix'], count=r['occurrence_count']) for r in db.execute("SELECT suffix,occurrence_count FROM hashes WHERE prefix=? ORDER BY suffix", (prefix,))]

    @contextmanager
    def reader(self):
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute('PRAGMA query_only=ON')
        try:
            yield connection
        finally:
            connection.close()

    def stats(self):
        with self.lock:
            if self._stats_cache is None:
                sources = [dict(r) for r in self.db.execute("SELECT source,COUNT(*) AS records FROM hashes GROUP BY source")]
                total = self.db.execute("SELECT COUNT(*) FROM hashes").fetchone()[0]
                self._stats_cache = dict(sources=sources,total_hashes=total)
            batches = [dict(r) for r in self.db.execute("SELECT * FROM processed_batches ORDER BY applied_at DESC LIMIT 30")]
            return dict(**self._stats_cache,
                        coverage="Subset corpus; synthetic data is labelled by source. Absence is not proof of safety.",
                        indexes=self.index_status(), batches=batches)
