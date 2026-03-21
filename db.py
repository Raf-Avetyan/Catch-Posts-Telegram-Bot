import sqlite3
from pathlib import Path
from typing import Optional


class Database:
    def __init__(self, db_path: Path):
        self.db_path = str(db_path)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    message_id INTEGER NOT NULL,
                    text TEXT,
                    media_path TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(channel, message_id)
                )
                """
            )
            conn.commit()

    def insert_post(
        self,
        source: str,
        channel: str,
        message_id: int,
        text: str,
        media_path: Optional[str],
        created_at: str,
    ) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO posts (
                    source,
                    channel,
                    message_id,
                    text,
                    media_path,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (source, channel, message_id, text, media_path, created_at),
            )
            conn.commit()
            return cursor.rowcount > 0
