import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable


class BridgeReceiptStore:
    def __init__(
        self,
        path: str,
        retention_seconds: int = 7 * 24 * 60 * 60,
        now_fn: Callable[[], float] = time.time,
    ):
        self.retention_seconds = max(1, int(retention_seconds))
        self._now = now_fn
        self._lock = threading.RLock()

        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(
            path,
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
        )
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("PRAGMA busy_timeout=30000")
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_messages (
                dedup_key TEXT PRIMARY KEY,
                processed_at REAL NOT NULL,
                expires_at REAL NOT NULL
            )
            """
        )

    def _cleanup_locked(self, now: float) -> None:
        self._db.execute(
            "DELETE FROM processed_messages WHERE expires_at <= ?",
            (now,),
        )

    def is_processed(self, dedup_key: str) -> bool:
        now = self._now()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self._cleanup_locked(now)
                row = self._db.execute(
                    "SELECT 1 FROM processed_messages WHERE dedup_key=?",
                    (str(dedup_key),),
                ).fetchone()
                self._db.execute("COMMIT")
            except Exception:
                self._db.execute("ROLLBACK")
                raise
        return row is not None

    def record_processed(self, dedup_key: str) -> None:
        now = self._now()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self._cleanup_locked(now)
                self._db.execute(
                    """
                    INSERT INTO processed_messages(
                        dedup_key, processed_at, expires_at
                    ) VALUES(?, ?, ?)
                    ON CONFLICT(dedup_key) DO UPDATE SET
                        processed_at=excluded.processed_at,
                        expires_at=excluded.expires_at
                    """,
                    (
                        str(dedup_key),
                        now,
                        now + self.retention_seconds,
                    ),
                )
                self._db.execute("COMMIT")
            except Exception:
                self._db.execute("ROLLBACK")
                raise

    def close(self) -> None:
        with self._lock:
            self._db.close()
