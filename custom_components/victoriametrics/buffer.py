"""Thread-safe, durable metric queue. All methods are called off the HA loop."""
import sqlite3
import threading
from pathlib import Path


class MetricBuffer:
    def __init__(self, path, max_records, max_bytes):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.max_records = max_records
        self.max_bytes = max_bytes
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT NOT NULL, size INTEGER NOT NULL, created REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
            INSERT OR IGNORE INTO counters VALUES ('dropped_records', 0);
            INSERT OR REPLACE INTO counters SELECT 'pending_records', COUNT(*) FROM queue;
            INSERT OR REPLACE INTO counters SELECT 'pending_bytes', COALESCE(SUM(size),0) FROM queue;
            CREATE INDEX IF NOT EXISTS queue_created ON queue(created);
            CREATE TRIGGER IF NOT EXISTS queue_insert AFTER INSERT ON queue BEGIN
                UPDATE counters SET value=value+1 WHERE name='pending_records';
                UPDATE counters SET value=value+NEW.size WHERE name='pending_bytes';
            END;
            CREATE TRIGGER IF NOT EXISTS queue_delete AFTER DELETE ON queue BEGIN
                UPDATE counters SET value=value-1 WHERE name='pending_records';
                UPDATE counters SET value=value-OLD.size WHERE name='pending_bytes';
            END;
        ''')
        self.db.commit()

    def append(self, metrics, created):
        with self.lock, self.db:
            self.db.executemany(
                'INSERT INTO queue(payload,size,created) VALUES (?,?,?)',
                [(p, len(p.encode('utf-8')), created) for p in metrics],
            )
            counters = dict(self.db.execute('SELECT name,value FROM counters'))
            count, size = counters['pending_records'], counters['pending_bytes']
            dropped = 0
            # Evict oldest first; SQLite reuses freed pages on subsequent inserts.
            while count > self.max_records or size > self.max_bytes:
                row_id, row_size = self.db.execute('SELECT id,size FROM queue ORDER BY id LIMIT 1').fetchone()
                self.db.execute('DELETE FROM queue WHERE id=?', (row_id,))
                count -= 1
                size -= row_size
                dropped += 1
            self.db.execute("UPDATE counters SET value=value+? WHERE name='dropped_records'", (dropped,))
            return dropped

    def batch(self, limit):
        with self.lock:
            return self.db.execute('SELECT id,payload FROM queue ORDER BY id LIMIT ?', (limit,)).fetchall()

    def remove(self, rows, dropped=False):
        with self.lock, self.db:
            removed = self.db.executemany('DELETE FROM queue WHERE id=?', [(r[0],) for r in rows]).rowcount
            if dropped:
                self.db.execute("UPDATE counters SET value=value+? WHERE name='dropped_records'", (removed,))

    def stats(self):
        with self.lock:
            stats = dict(self.db.execute('SELECT name,value FROM counters'))
            stats['oldest_timestamp'] = self.db.execute('SELECT MIN(created) FROM queue').fetchone()[0]
            return stats

    def close(self):
        with self.lock:
            self.db.close()
