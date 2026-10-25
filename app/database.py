"""
Database Layer with SQLite for persistence of Configuration, Checkpoints, Transfers, and Logs.
"""
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import aiosqlite

from app.models import (
    AppConfig,
    CheckpointRecord,
    CodexConfig,
    ChatGPTConfig,
    EventLogRecord,
    RouteConfig,
    ShuttleStage,
    compute_sha256,
)

SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS configurations (
    key TEXT PRIMARY KEY,
    value JSON NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage TEXT NOT NULL,
    iteration_id INTEGER NOT NULL DEFAULT 0,
    source_identifier TEXT NOT NULL,
    destination_identifier TEXT NOT NULL,
    content_hash TEXT,
    payload_preview TEXT,
    transfer_status TEXT NOT NULL DEFAULT 'pending',
    failure_details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transfers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    iteration_id INTEGER NOT NULL DEFAULT 0,
    direction TEXT NOT NULL, -- 'codex_to_chatgpt' or 'chatgpt_to_codex'
    source_identifier TEXT NOT NULL,
    destination_identifier TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    payload_preview TEXT,
    status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'sent', 'confirmed', 'failed', 'unknown'
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS event_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'info',
    source TEXT NOT NULL DEFAULT 'system',
    stage TEXT NOT NULL,
    message TEXT NOT NULL,
    details JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_created ON checkpoints(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_transfers_hash ON transfers(content_hash);
CREATE INDEX IF NOT EXISTS idx_event_logs_stage ON event_logs(stage);
"""


class Database:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)

    async def init(self):
        """Run schema migration and ensure database structure."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(SCHEMA_V1)
            # Record schema version 1
            cursor = await db.execute("SELECT MAX(version) FROM schema_version")
            row = await cursor.fetchone()
            current_ver = row[0] if row and row[0] is not None else 0
            if current_ver < 1:
                await db.execute("INSERT INTO schema_version (version) VALUES (1)")
            await db.commit()

    async def save_config(self, config: AppConfig):
        async with aiosqlite.connect(self.db_path) as db:
            data = config.model_dump(mode="json")
            await db.execute(
                "INSERT INTO configurations (key, value, updated_at) VALUES ('app_config', ?, CURRENT_TIMESTAMP) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
                (json.dumps(data),),
            )
            await db.commit()

    async def get_config(self) -> Optional[AppConfig]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT value FROM configurations WHERE key = 'app_config'")
            row = await cursor.fetchone()
            if not row:
                return None
            try:
                data = json.loads(row[0])
                return AppConfig(**data)
            except Exception:
                return None

    async def save_checkpoint(self, record: CheckpointRecord) -> CheckpointRecord:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                INSERT INTO checkpoints (
                    stage, iteration_id, source_identifier, destination_identifier,
                    content_hash, payload_preview, transfer_status, failure_details, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    record.stage.value,
                    record.iteration_id,
                    record.source_identifier,
                    record.destination_identifier,
                    record.content_hash,
                    record.payload_preview,
                    record.transfer_status,
                    record.failure_details,
                ),
            )
            record.id = cursor.lastrowid
            await db.commit()
            return record

    async def get_latest_checkpoint(self) -> Optional[CheckpointRecord]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT id, stage, iteration_id, source_identifier, destination_identifier,
                       content_hash, payload_preview, transfer_status, failure_details,
                       created_at, updated_at
                FROM checkpoints ORDER BY id DESC LIMIT 1
                """
            )
            row = await cursor.fetchone()
            if not row:
                return None
            return CheckpointRecord(
                id=row[0],
                stage=ShuttleStage(row[1]),
                iteration_id=row[2],
                source_identifier=row[3],
                destination_identifier=row[4],
                content_hash=row[5],
                payload_preview=row[6],
                transfer_status=row[7],
                failure_details=row[8],
                created_at=datetime.fromisoformat(row[9]) if isinstance(row[9], str) else row[9],
                updated_at=datetime.fromisoformat(row[10]) if isinstance(row[10], str) else row[10],
            )

    async def record_transfer(
        self,
        iteration_id: int,
        direction: str,
        source_id: str,
        dest_id: str,
        content_hash: str,
        payload_preview: Optional[str],
        status: str = "pending",
        error_message: Optional[str] = None,
    ) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                INSERT INTO transfers (
                    iteration_id, direction, source_identifier, destination_identifier,
                    content_hash, payload_preview, status, error_message, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    iteration_id,
                    direction,
                    source_id,
                    dest_id,
                    content_hash,
                    payload_preview,
                    status,
                    error_message,
                ),
            )
            transfer_id = cursor.lastrowid
            await db.commit()
            return transfer_id

    async def find_transfer_by_hash(self, content_hash: str) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM transfers WHERE content_hash = ? ORDER BY id DESC",
                (content_hash,),
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

    async def log_event(
        self,
        event_type: str,
        stage: ShuttleStage,
        message: str,
        severity: str = "info",
        source: str = "system",
        details: Optional[Dict[str, Any]] = None,
    ):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO event_logs (event_type, severity, source, stage, message, details)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event_type,
                    severity,
                    source,
                    stage.value,
                    message,
                    json.dumps(details) if details else None,
                ),
            )
            await db.commit()

    async def get_recent_logs(self, limit: int = 50) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM event_logs ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            rows = await cursor.fetchall()
            results = []
            for r in rows:
                item = dict(r)
                if item.get("details"):
                    try:
                        item["details"] = json.loads(item["details"])
                    except Exception:
                        pass
                results.append(item)
            return results
