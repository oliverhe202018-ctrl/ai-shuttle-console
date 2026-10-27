"""
Database Layer with SQLite for persistence of Configuration, Checkpoints, Transfers, and Logs.
"""
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import aiosqlite

from app.models import (
    AppConfig,
    CodexDeliveryClassification,
    CheckpointRecord,
    CodexConfig,
    ChatGPTConfig,
    EventLogRecord,
    ObservationRecord,
    PromptExtractionRecord,
    RouteConfig,
    ShuttleStage,
    TransferAuthorization,
    TransferDirection,
    TransferRecord,
    TransferStatus,
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

CREATE TABLE IF NOT EXISTS observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL, -- 'codex' or 'chatgpt'
    source_identifier TEXT NOT NULL,
    message_id TEXT,
    event_offset INTEGER,
    content_hash TEXT NOT NULL,
    payload_preview TEXT,
    raw_payload_snippet TEXT,
    status TEXT NOT NULL DEFAULT 'verified',
    error_details TEXT,
    observed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS prompt_extractions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_conversation_id TEXT NOT NULL,
    source_message_id TEXT,
    observation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    content_hash TEXT NOT NULL,
    extracted_prompt_hash TEXT NOT NULL,
    extracted_prompt_preview TEXT NOT NULL,
    raw_response_snippet TEXT,
    provenance_policy TEXT NOT NULL,
    is_fresh BOOLEAN NOT NULL DEFAULT 1,
    transfer_eligible BOOLEAN NOT NULL DEFAULT 0,
    ineligibility_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_created ON checkpoints(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_transfers_hash ON transfers(content_hash);
CREATE INDEX IF NOT EXISTS idx_event_logs_stage ON event_logs(stage);
CREATE INDEX IF NOT EXISTS idx_observations_src ON observations(source_type, source_identifier);
CREATE INDEX IF NOT EXISTS idx_observations_hash ON observations(content_hash);
CREATE INDEX IF NOT EXISTS idx_extractions_hash ON prompt_extractions(content_hash);
"""


class Database:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)

    async def init(self):
        """Run schema migration and ensure database structure."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(SCHEMA_V1)

            # Phase 2.3 schema migration: transfer_authorizations & enhanced transfers columns
            await db.execute("""
            CREATE TABLE IF NOT EXISTS transfer_authorizations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transfer_id TEXT NOT NULL UNIQUE,
                confirmation_token TEXT NOT NULL UNIQUE,
                destination_identifier TEXT NOT NULL,
                expected_content_hash TEXT NOT NULL,
                expected_baseline_hash TEXT,
                consumed BOOLEAN NOT NULL DEFAULT 0,
                expires_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                config_fingerprint TEXT
            );
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_trans_auth_token ON transfer_authorizations(confirmation_token);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_trans_auth_tid ON transfer_authorizations(transfer_id);")

            cursor = await db.execute("PRAGMA table_info(transfer_authorizations)")
            existing_auth_cols = {row[1] for row in await cursor.fetchall()}
            if "config_fingerprint" not in existing_auth_cols:
                await db.execute("ALTER TABLE transfer_authorizations ADD COLUMN config_fingerprint TEXT")

            # Inspect transfers columns and add any missing Phase 2.3 columns
            cursor = await db.execute("PRAGMA table_info(transfers)")
            existing_cols = {row[1] for row in await cursor.fetchall()}

            new_cols = [
                ("transfer_id", "TEXT"),
                ("source_type", "TEXT DEFAULT 'codex'"),
                ("source_message_id", "TEXT"),
                ("destination_type", "TEXT DEFAULT 'chatgpt'"),
                ("char_count", "INTEGER DEFAULT 0"),
                ("message_text", "TEXT"),
                ("baseline_message_id", "TEXT"),
                ("baseline_hash", "TEXT"),
                ("delivery_evidence", "JSON"),
                ("human_confirmed", "BOOLEAN DEFAULT 0"),
                ("confirmation_token", "TEXT"),
                ("is_operator_modified", "BOOLEAN DEFAULT 0"),
                ("provenance_policy", "TEXT"),
                ("confirmed_at", "TIMESTAMP"),
                ("sent_at", "TIMESTAMP"),
                ("verified_at", "TIMESTAMP"),
                ("config_fingerprint", "TEXT"),
                ("raw_content_hash", "TEXT"),
                ("semantic_content_hash", "TEXT"),
            ]
            for col_name, col_def in new_cols:
                if col_name not in existing_cols:
                    await db.execute(f"ALTER TABLE transfers ADD COLUMN {col_name} {col_def}")

            await db.execute("CREATE INDEX IF NOT EXISTS idx_transfers_tid ON transfers(transfer_id);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_transfers_status ON transfers(status);")

            # Phase 2.5 schema: canary_rounds
            await db.execute("""
            CREATE TABLE IF NOT EXISTS canary_rounds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_id TEXT NOT NULL UNIQUE,
                round_number INTEGER NOT NULL,
                nonce TEXT NOT NULL UNIQUE,
                c2g_transfer_id TEXT,
                chatgpt_user_turn_id TEXT,
                chatgpt_assistant_turn_id TEXT,
                g2c_transfer_id TEXT,
                codex_queue_receipt_id TEXT,
                codex_user_turn_id TEXT,
                codex_assistant_message_id TEXT,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                result TEXT NOT NULL,
                manual_thread_load_required BOOLEAN DEFAULT 0,
                metrics JSON,
                error_details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_canary_rounds_rid ON canary_rounds(round_id);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_canary_rounds_nonce ON canary_rounds(nonce);")

            cr_cols_cursor = await db.execute("PRAGMA table_info(canary_rounds);")
            cr_existing_cols = {col[1] for col in await cr_cols_cursor.fetchall()}
            cr_new_cols = [
                ("is_recovered", "BOOLEAN DEFAULT 0"),
                ("recovery_notes", "TEXT"),
                ("recovered_at", "TIMESTAMP"),
            ]
            for col_name, col_def in cr_new_cols:
                if col_name not in cr_existing_cols:
                    await db.execute(f"ALTER TABLE canary_rounds ADD COLUMN {col_name} {col_def}")

            # Record schema version
            cursor = await db.execute("SELECT MAX(version) FROM schema_version")
            row = await cursor.fetchone()
            current_ver = row[0] if row and row[0] is not None else 0
            if current_ver < 2:
                await db.execute("INSERT INTO schema_version (version) VALUES (2)")
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

    async def save_canary_bootstrap_state(self, state: Dict[str, Any]):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO configurations (key, value, updated_at) VALUES ('canary_bootstrap_state', ?, CURRENT_TIMESTAMP) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
                (json.dumps(state, default=str),),
            )
            await db.commit()

    async def get_canary_bootstrap_state(self) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT value FROM configurations WHERE key = 'canary_bootstrap_state'")
            row = await cursor.fetchone()
            if not row:
                return None
            try:
                return json.loads(row[0])
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

    async def get_event_logs(self, limit: int = 50) -> List[Dict[str, Any]]:
        return await self.get_recent_logs(limit=limit)

    async def record_observation(self, obs: ObservationRecord) -> ObservationRecord:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                INSERT INTO observations (
                    source_type, source_identifier, message_id, event_offset,
                    content_hash, payload_preview, raw_payload_snippet, status,
                    error_details, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    obs.source_type,
                    obs.source_identifier,
                    obs.message_id,
                    obs.event_offset,
                    obs.content_hash,
                    obs.payload_preview,
                    obs.raw_payload_snippet,
                    obs.status,
                    obs.error_details,
                ),
            )
            obs.id = cursor.lastrowid
            await db.commit()
            return obs

    async def get_latest_observation(
        self, source_type: str, source_identifier: Optional[str] = None
    ) -> Optional[ObservationRecord]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            if source_identifier:
                cursor = await db.execute(
                    """
                    SELECT * FROM observations
                    WHERE source_type = ? AND source_identifier = ?
                    ORDER BY id DESC LIMIT 1
                    """,
                    (source_type, source_identifier),
                )
            else:
                cursor = await db.execute(
                    """
                    SELECT * FROM observations
                    WHERE source_type = ?
                    ORDER BY id DESC LIMIT 1
                    """,
                    (source_type,),
                )
            row = await cursor.fetchone()
            if not row:
                return None
            return ObservationRecord(
                id=row["id"],
                source_type=row["source_type"],
                source_identifier=row["source_identifier"],
                message_id=row["message_id"],
                event_offset=row["event_offset"],
                content_hash=row["content_hash"],
                payload_preview=row["payload_preview"],
                raw_payload_snippet=row["raw_payload_snippet"],
                status=row["status"],
                error_details=row["error_details"],
                observed_at=datetime.fromisoformat(row["observed_at"]) if isinstance(row["observed_at"], str) else row["observed_at"],
            )

    async def record_prompt_extraction(self, ext: PromptExtractionRecord) -> PromptExtractionRecord:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                INSERT INTO prompt_extractions (
                    source_conversation_id, source_message_id, observation_timestamp,
                    content_hash, extracted_prompt_hash, extracted_prompt_preview,
                    raw_response_snippet, provenance_policy, is_fresh,
                    transfer_eligible, ineligibility_reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ext.source_conversation_id,
                    ext.source_message_id,
                    ext.observation_timestamp.isoformat() if ext.observation_timestamp else datetime.utcnow().isoformat(),
                    ext.content_hash,
                    ext.extracted_prompt_hash,
                    ext.extracted_prompt_preview,
                    ext.raw_response_snippet,
                    ext.provenance_policy,
                    1 if ext.is_fresh else 0,
                    1 if ext.transfer_eligible else 0,
                    ext.ineligibility_reason,
                    ext.created_at.isoformat() if ext.created_at else datetime.utcnow().isoformat(),
                ),
            )
            ext.id = cursor.lastrowid
            await db.commit()
            return ext

    async def has_seen_content_hash(self, content_hash: str) -> bool:
        """Check if content hash was previously recorded in observations or prompt extractions."""
        if not content_hash:
            return False
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT 1 FROM observations WHERE content_hash = ? LIMIT 1",
                (content_hash,),
            )
            if await cursor.fetchone():
                return True
            cursor2 = await db.execute(
                "SELECT 1 FROM prompt_extractions WHERE content_hash = ? OR extracted_prompt_hash = ? LIMIT 1",
                (content_hash, content_hash),
            )
            return await cursor2.fetchone() is not None

    async def get_latest_prompt_extraction(
        self, source_conversation_id: Optional[str] = None
    ) -> Optional[PromptExtractionRecord]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            if source_conversation_id:
                cursor = await db.execute(
                    "SELECT * FROM prompt_extractions WHERE source_conversation_id = ? ORDER BY id DESC LIMIT 1",
                    (source_conversation_id,),
                )
            else:
                cursor = await db.execute(
                    "SELECT * FROM prompt_extractions ORDER BY id DESC LIMIT 1"
                )
            row = await cursor.fetchone()
            if not row:
                return None
            return PromptExtractionRecord(
                id=row["id"],
                source_conversation_id=row["source_conversation_id"],
                source_message_id=row["source_message_id"],
                observation_timestamp=datetime.fromisoformat(row["observation_timestamp"]) if isinstance(row["observation_timestamp"], str) else row["observation_timestamp"],
                content_hash=row["content_hash"],
                extracted_prompt_hash=row["extracted_prompt_hash"],
                extracted_prompt_preview=row["extracted_prompt_preview"],
                raw_response_snippet=row["raw_response_snippet"],
                provenance_policy=row["provenance_policy"],
                is_fresh=bool(row["is_fresh"]),
                transfer_eligible=bool(row["transfer_eligible"]),
                ineligibility_reason=row["ineligibility_reason"],
                created_at=datetime.fromisoformat(row["created_at"]) if isinstance(row["created_at"], str) else row["created_at"],
                config_fingerprint=row["config_fingerprint"] if "config_fingerprint" in row.keys() else None,
            )

    async def save_transfer_record(self, record: TransferRecord) -> TransferRecord:
        """Save or update full transfer record."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT id FROM transfers WHERE transfer_id = ?", (record.transfer_id,))
            row = await cursor.fetchone()

            if row:
                record.id = row["id"]
                await db.execute(
                    """
                    UPDATE transfers SET
                        iteration_id = ?, direction = ?, source_type = ?, source_identifier = ?,
                        source_message_id = ?, destination_type = ?, destination_identifier = ?,
                        content_hash = ?, char_count = ?, message_text = ?, payload_preview = ?,
                        baseline_message_id = ?, baseline_hash = ?, status = ?, delivery_evidence = ?,
                        error_message = ?, human_confirmed = ?, confirmation_token = ?,
                        is_operator_modified = ?, provenance_policy = ?, confirmed_at = ?,
                        sent_at = ?, verified_at = ?, config_fingerprint = ?, raw_content_hash = ?,
                        semantic_content_hash = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE transfer_id = ?
                    """,
                    (
                        record.iteration_id, record.direction, record.source_type, record.source_identifier,
                        record.source_message_id, record.destination_type, record.destination_identifier,
                        record.content_hash, record.char_count, record.message_text, record.payload_preview,
                        record.baseline_message_id, record.baseline_hash, record.status,
                        json.dumps(record.delivery_evidence) if record.delivery_evidence else None,
                        record.error_message, 1 if record.human_confirmed else 0, record.confirmation_token,
                        1 if record.is_operator_modified else 0, record.provenance_policy,
                        record.confirmed_at.isoformat() if record.confirmed_at else None,
                        record.sent_at.isoformat() if record.sent_at else None,
                        record.verified_at.isoformat() if record.verified_at else None,
                        record.config_fingerprint,
                        record.raw_content_hash or record.content_hash,
                        record.semantic_content_hash,
                        record.transfer_id,
                    ),
                )
            else:
                cursor = await db.execute(
                    """
                    INSERT INTO transfers (
                        transfer_id, iteration_id, direction, source_type, source_identifier,
                        source_message_id, destination_type, destination_identifier, content_hash,
                        char_count, message_text, payload_preview, baseline_message_id,
                        baseline_hash, status, delivery_evidence, error_message, human_confirmed,
                        confirmation_token, is_operator_modified, provenance_policy, confirmed_at,
                        sent_at, verified_at, config_fingerprint, raw_content_hash,
                        semantic_content_hash, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (
                        record.transfer_id, record.iteration_id, record.direction, record.source_type,
                        record.source_identifier, record.source_message_id, record.destination_type,
                        record.destination_identifier, record.content_hash, record.char_count,
                        record.message_text, record.payload_preview, record.baseline_message_id,
                        record.baseline_hash, record.status,
                        json.dumps(record.delivery_evidence) if record.delivery_evidence else None,
                        record.error_message, 1 if record.human_confirmed else 0, record.confirmation_token,
                        1 if record.is_operator_modified else 0, record.provenance_policy,
                        record.confirmed_at.isoformat() if record.confirmed_at else None,
                        record.sent_at.isoformat() if record.sent_at else None,
                        record.verified_at.isoformat() if record.verified_at else None,
                        record.config_fingerprint,
                        record.raw_content_hash or record.content_hash,
                        record.semantic_content_hash,
                    ),
                )
                record.id = cursor.lastrowid
            await db.commit()
            return record

    async def get_transfer_by_id(self, transfer_id: str) -> Optional[TransferRecord]:
        """Retrieve transfer record by transfer_id."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM transfers WHERE transfer_id = ?", (transfer_id,))
            row = await cursor.fetchone()
            if not row:
                return None
            return self._row_to_transfer(row)


    async def get_all_transfers(self, limit: int = 50) -> List[TransferRecord]:
        """Retrieve recent transfer records."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM transfers ORDER BY id DESC LIMIT ?", (limit,)
            )
            rows = await cursor.fetchall()
            return [self._row_to_transfer(r) for r in rows]

    async def get_latest_transfer(self, direction: Optional[str] = None) -> Optional[TransferRecord]:
        """Retrieve latest transfer record, optionally filtered by direction."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            if direction:
                cursor = await db.execute(
                    "SELECT * FROM transfers WHERE direction = ? ORDER BY id DESC LIMIT 1", (direction,)
                )
            else:
                cursor = await db.execute("SELECT * FROM transfers ORDER BY id DESC LIMIT 1")
            row = await cursor.fetchone()
            if not row:
                return None
            return self._row_to_transfer(row)

    async def get_unresolved_deliveries(self) -> List[TransferRecord]:
        """
        Retrieve all transfers whose delivery outcome is genuinely unresolved.
        Only active non-terminal delivery states qualify ('sending', 'delivery_outcome_unknown').
        Terminal/resolved states (delivered, cancelled, failed, not_sent) are excluded.
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM transfers WHERE status IN ('sending', 'delivery_outcome_unknown') ORDER BY id DESC"
            )
            rows = await cursor.fetchall()
            return [self._row_to_transfer(r) for r in rows]

    async def reconcile_unknown_outcome_transaction(
        self,
        transfer_id: str,
        resolution: str,
        notes: Optional[str] = None,
        delivery_evidence_update: Optional[Dict[str, Any]] = None,
    ) -> Tuple[TransferRecord, CheckpointRecord]:
        """
        Atomically reconcile an unknown delivery outcome in a single transaction.
        Updates the transfer record, invalidates tokens if cancelled, inserts the new checkpoint
        (clearing failure_details), and writes audit event logs.
        """
        if resolution not in ("mark_delivered", "mark_cancelled"):
            raise ValueError(f"Invalid resolution '{resolution}'; must be 'mark_delivered' or 'mark_cancelled'")

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE")
            try:
                # 1. Fetch transfer
                cursor = await db.execute("SELECT * FROM transfers WHERE transfer_id = ?", (transfer_id,))
                row = await cursor.fetchone()
                if not row:
                    raise KeyError(f"Transfer '{transfer_id}' not found")
                transfer = self._row_to_transfer(row)

                # Fetch current checkpoint
                cp_cur = await db.execute("SELECT * FROM checkpoints ORDER BY id DESC LIMIT 1")
                cp_row = await cp_cur.fetchone()
                cur_stage = ShuttleStage(cp_row["stage"]) if cp_row else ShuttleStage.IDLE
                cur_iter = cp_row["iteration_id"] if cp_row else 1

                if transfer.status not in (TransferStatus.DELIVERY_OUTCOME_UNKNOWN.value, TransferStatus.SENDING.value) and cur_stage != ShuttleStage.DELIVERY_OUTCOME_UNKNOWN:
                    raise ValueError(f"Transfer '{transfer_id}' is in status '{transfer.status}' and stage is '{cur_stage.value}', not in DELIVERY_OUTCOME_UNKNOWN state")

                now_iso = datetime.utcnow().isoformat()
                now_dt = datetime.utcnow()

                if resolution == "mark_delivered":
                    new_status = TransferStatus.DELIVERED.value
                    evidence = transfer.delivery_evidence or {}
                    evidence["reconciled_by"] = "operator"
                    evidence["notes"] = notes
                    evidence["timestamp"] = now_iso
                    evidence["resolution"] = "mark_delivered"
                    new_stage = (
                        ShuttleStage.CHATGPT_WAITING
                        if transfer.direction == TransferDirection.CODEX_TO_CHATGPT.value
                        else ShuttleStage.CODEX_WAITING
                    )
                    if transfer.direction == TransferDirection.CODEX_TO_CHATGPT.value:
                        evidence.setdefault("raw_content_hash", transfer.content_hash)
                        if getattr(transfer, "semantic_content_hash", None):
                            evidence.setdefault("semantic_content_hash", transfer.semantic_content_hash)
                        evidence.setdefault("semantic_delivery_status", "SEMANTIC_DELIVERY_CONFIRMED")
                        evidence.setdefault("audit_trail", "delivery initially ambiguous -> independently verified delivered")
                        c_cur = await db.execute("SELECT round_id, error_details FROM canary_rounds WHERE c2g_transfer_id = ?", (transfer.transfer_id,))
                        c_row = await c_cur.fetchone()
                        if c_row:
                            r_id = c_row["round_id"]
                            orig_err = c_row["error_details"] or transfer.error_message or "C2G delivery confirmation timed out"
                            new_err_details = f"{orig_err}. Independently verified delivered: delivery initially ambiguous -> independently verified delivered"
                            u_turn = (delivery_evidence_update or {}).get("user_turn_id") or evidence.get("user_turn_id")
                            a_turn = (delivery_evidence_update or {}).get("assistant_turn_id") or evidence.get("assistant_turn_id")
                            await db.execute(
                                """
                                UPDATE canary_rounds
                                SET result = 'ROUND_RECOVERED_AFTER_FALSE_UNKNOWN',
                                    is_recovered = 1,
                                    recovery_notes = ?,
                                    recovered_at = ?,
                                    chatgpt_user_turn_id = COALESCE(?, chatgpt_user_turn_id),
                                    chatgpt_assistant_turn_id = COALESCE(?, chatgpt_assistant_turn_id),
                                    error_details = ?
                                WHERE round_id = ?
                                """,
                                (
                                    notes or "Independently verified delivered via post-send semantic delivery confirmation repair",
                                    now_iso,
                                    u_turn,
                                    a_turn,
                                    new_err_details,
                                    r_id,
                                ),
                            )
                    if transfer.direction == TransferDirection.CHATGPT_TO_CODEX.value:
                        if "queue_receipt_status" not in evidence:
                            evidence["queue_receipt_status"] = "accepted"
                        if "turn_dispatch_status" not in evidence:
                            evidence["turn_dispatch_status"] = "pending"
                        if "task_completion_status" not in evidence:
                            evidence["task_completion_status"] = "pending"
                        if "outcome_classification" not in evidence:
                            evidence["outcome_classification"] = CodexDeliveryClassification.CODEX_QUEUE_ACCEPTED_NOT_DISPATCHED.value
                        try:
                            from app.adapters import inspect_codex_queue_storage
                            q_store = inspect_codex_queue_storage(transfer.destination_identifier, expected_content_hash=transfer.content_hash)
                            if q_store.get("has_matching_item"):
                                first_item = q_store.get("first_matching_item") or {}
                                if first_item.get("id") and "receipt_id" not in evidence:
                                    evidence["receipt_id"] = first_item.get("id")
                                if first_item.get("client_id") and "client_user_message_id" not in evidence:
                                    evidence["client_user_message_id"] = first_item.get("client_id")
                        except Exception:
                            pass
                    if delivery_evidence_update:
                        evidence.update(delivery_evidence_update)

                    if transfer.direction == TransferDirection.CHATGPT_TO_CODEX.value:
                        if (
                            evidence.get("task_completion_status") == "completed"
                            or evidence.get("outcome_classification") == CodexDeliveryClassification.CODEX_QUEUE_AND_TASK_COMPLETED.value
                        ):
                            evidence["queue_receipt_status"] = "accepted"
                            evidence["turn_dispatch_status"] = "dispatched"
                            evidence["task_completion_status"] = "completed"
                            evidence["outcome_classification"] = CodexDeliveryClassification.CODEX_QUEUE_AND_TASK_COMPLETED.value
                            new_stage = ShuttleStage.IDLE

                    transfer_status = "reconciled_delivered"
                    transfer.status = new_status
                    transfer.delivery_evidence = evidence
                    transfer.error_message = None
                    effective_sent_at = transfer.sent_at or transfer.confirmed_at or transfer.created_at or now_dt
                    sent_at_str = effective_sent_at.isoformat() if hasattr(effective_sent_at, "isoformat") else str(effective_sent_at)
                    transfer.sent_at = datetime.fromisoformat(sent_at_str) if isinstance(effective_sent_at, str) else effective_sent_at

                    await db.execute(
                        """
                        UPDATE transfers
                        SET status = ?, delivery_evidence = ?, error_message = ?, sent_at = COALESCE(sent_at, ?), updated_at = ?
                        WHERE transfer_id = ?
                        """,
                        (
                            new_status,
                            json.dumps(evidence),
                            None,
                            sent_at_str,
                            now_iso,
                            transfer_id,
                        ),
                    )
                else:  # mark_cancelled
                    new_status = TransferStatus.CANCELLED.value
                    err_msg = f"Cancelled by operator: {notes or 'No reason provided'}"
                    cfg = await self.get_config()
                    is_canary_mode = bool(cfg and cfg.canary_mode)
                    if is_canary_mode:
                        new_stage = (
                            ShuttleStage.CODEX_WAITING
                            if transfer.direction == TransferDirection.CODEX_TO_CHATGPT.value
                            else ShuttleStage.CHATGPT_WAITING
                        )
                    else:
                        new_stage = (
                            ShuttleStage.REPORT_PREVIEW
                            if transfer.direction == TransferDirection.CODEX_TO_CHATGPT.value
                            else ShuttleStage.PROMPT_PREVIEW
                        )
                    evidence = transfer.delivery_evidence or {}
                    evidence["reconciled_by"] = "operator"
                    evidence["notes"] = notes
                    evidence["timestamp"] = now_iso
                    evidence["resolution"] = "mark_cancelled"
                    if transfer.direction == TransferDirection.CHATGPT_TO_CODEX.value:
                        evidence["queue_receipt_status"] = "rejected"
                        evidence["turn_dispatch_status"] = "not_dispatched"
                        evidence["task_completion_status"] = "not_started"
                        evidence["outcome_classification"] = CodexDeliveryClassification.CODEX_NOT_SENT.value
                    if delivery_evidence_update:
                        evidence.update(delivery_evidence_update)

                    transfer_status = "cancelled"
                    transfer.status = new_status
                    transfer.delivery_evidence = evidence
                    transfer.error_message = err_msg

                    await db.execute(
                        """
                        UPDATE transfers
                        SET status = ?, delivery_evidence = ?, error_message = ?, updated_at = ?
                        WHERE transfer_id = ?
                        """,
                        (
                            new_status,
                            json.dumps(evidence),
                            err_msg,
                            now_iso,
                            transfer_id,
                        ),
                    )
                    if transfer.confirmation_token:
                        await db.execute(
                            "UPDATE transfer_authorizations SET consumed = 1 WHERE confirmation_token = ?",
                            (transfer.confirmation_token,),
                        )

                # 2. Insert new checkpoint with cleared failure_details
                cp_cursor = await db.execute(
                    """
                    INSERT INTO checkpoints (
                        stage, iteration_id, source_identifier, destination_identifier,
                        content_hash, payload_preview, transfer_status, failure_details, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_stage.value,
                        cur_iter,
                        transfer.source_identifier,
                        transfer.destination_identifier,
                        transfer.content_hash,
                        transfer.payload_preview,
                        transfer_status,
                        None,
                        now_iso,
                        now_iso,
                    ),
                )
                new_cp_id = cp_cursor.lastrowid
                new_checkpoint = CheckpointRecord(
                    id=new_cp_id,
                    stage=new_stage,
                    iteration_id=cur_iter,
                    source_identifier=transfer.source_identifier,
                    destination_identifier=transfer.destination_identifier,
                    content_hash=transfer.content_hash,
                    payload_preview=transfer.payload_preview,
                    transfer_status=transfer_status,
                    failure_details=None,
                    created_at=now_dt,
                    updated_at=now_dt,
                )

                # 3. Insert audit events
                await db.execute(
                    """
                    INSERT INTO event_logs (event_type, severity, source, stage, message, details, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "STAGE_TRANSITION",
                        "info",
                        "system",
                        new_stage.value,
                        f"Transitioned from {cur_stage.value} to {new_stage.value}",
                        json.dumps({"iteration": cur_iter, "transfer_status": transfer_status}),
                        now_iso,
                    ),
                )
                await db.execute(
                    """
                    INSERT INTO event_logs (event_type, severity, source, stage, message, details, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "UNKNOWN_OUTCOME_RECONCILED",
                        "info",
                        "system",
                        new_stage.value,
                        f"Unknown outcome reconciled as {resolution} for transfer {transfer_id}",
                        json.dumps({"transfer_id": transfer_id, "resolution": resolution, "notes": notes}),
                        now_iso,
                    ),
                )

                if (
                    resolution == "mark_delivered"
                    and transfer.direction == TransferDirection.CHATGPT_TO_CODEX.value
                    and (
                        evidence.get("task_completion_status") == "completed"
                        or evidence.get("outcome_classification") == CodexDeliveryClassification.CODEX_QUEUE_AND_TASK_COMPLETED.value
                    )
                ):
                    canary_nonce = None
                    if transfer.provenance_policy and transfer.provenance_policy.startswith("canary:"):
                        canary_nonce = transfer.provenance_policy.split("canary:", 1)[1]
                    c2g_cur = await db.execute("SELECT transfer_id FROM transfers WHERE direction = 'codex_to_chatgpt' AND status = 'delivered' ORDER BY id DESC LIMIT 1")
                    c2g_row = await c2g_cur.fetchone()
                    c2g_tid = c2g_row["transfer_id"] if c2g_row else None
                    e2e_details = {
                        "transfer_id": transfer_id,
                        "g2c_transfer_id": transfer_id,
                        "c2g_transfer_id": c2g_tid,
                        "nonce": canary_nonce,
                        "queue_receipt_status": "accepted",
                        "turn_dispatch_status": "dispatched",
                        "task_completion_status": "completed",
                        "outcome_classification": CodexDeliveryClassification.CODEX_QUEUE_AND_TASK_COMPLETED.value,
                        "receipt_id": evidence.get("receipt_id"),
                        "client_user_message_id": evidence.get("client_user_message_id"),
                        "turn_id": evidence.get("turn_id"),
                        "assistant_reply_text": evidence.get("assistant_reply_text") or evidence.get("final_answer"),
                    }
                    await db.execute(
                        """
                        INSERT INTO event_logs (event_type, severity, source, stage, message, details, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "CANARY_END_TO_END_COMPLETED",
                            "info",
                            "system",
                            new_stage.value,
                            f"Canary cycle completed end-to-end for nonce {canary_nonce} with transfer {transfer_id}",
                            json.dumps(e2e_details),
                            now_iso,
                        ),
                    )

                await db.commit()
                return transfer, new_checkpoint
            except Exception:
                await db.rollback()
                raise

    def _row_to_transfer(self, row: aiosqlite.Row) -> TransferRecord:
        keys = row.keys()
        delivery_ev = None
        if "delivery_evidence" in keys and row["delivery_evidence"]:
            try:
                delivery_ev = json.loads(row["delivery_evidence"])
            except Exception:
                pass

        return TransferRecord(
            id=row["id"],
            transfer_id=row["transfer_id"] if "transfer_id" in keys and row["transfer_id"] else f"legacy-{row['id']}",
            iteration_id=row["iteration_id"],
            direction=row["direction"],
            source_type=row["source_type"] if "source_type" in keys and row["source_type"] else "codex",
            source_identifier=row["source_identifier"],
            source_message_id=row["source_message_id"] if "source_message_id" in keys else None,
            destination_type=row["destination_type"] if "destination_type" in keys and row["destination_type"] else "chatgpt",
            destination_identifier=row["destination_identifier"],
            content_hash=row["content_hash"],
            char_count=row["char_count"] if "char_count" in keys and row["char_count"] else len(row["payload_preview"] or ""),
            message_text=row["message_text"] if "message_text" in keys else None,
            payload_preview=row["payload_preview"],
            baseline_message_id=row["baseline_message_id"] if "baseline_message_id" in keys else None,
            baseline_hash=row["baseline_hash"] if "baseline_hash" in keys else None,
            status=row["status"],
            delivery_evidence=delivery_ev,
            error_message=row["error_message"],
            human_confirmed=bool(row["human_confirmed"]) if "human_confirmed" in keys else False,
            confirmation_token=row["confirmation_token"] if "confirmation_token" in keys else None,
            is_operator_modified=bool(row["is_operator_modified"]) if "is_operator_modified" in keys else False,
            provenance_policy=row["provenance_policy"] if "provenance_policy" in keys else None,
            created_at=datetime.fromisoformat(row["created_at"]) if isinstance(row["created_at"], str) else row["created_at"],
            confirmed_at=datetime.fromisoformat(row["confirmed_at"]) if "confirmed_at" in keys and isinstance(row["confirmed_at"], str) else None,
            sent_at=datetime.fromisoformat(row["sent_at"]) if "sent_at" in keys and isinstance(row["sent_at"], str) else None,
            verified_at=datetime.fromisoformat(row["verified_at"]) if "verified_at" in keys and isinstance(row["verified_at"], str) else None,
            config_fingerprint=row["config_fingerprint"] if "config_fingerprint" in keys else None,
            raw_content_hash=row["raw_content_hash"] if "raw_content_hash" in keys and row["raw_content_hash"] else row["content_hash"],
            semantic_content_hash=row["semantic_content_hash"] if "semantic_content_hash" in keys else None,
        )

    async def create_transfer_authorization(self, auth: TransferAuthorization) -> TransferAuthorization:
        """Persist single-use transfer authorization."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                INSERT INTO transfer_authorizations (
                    transfer_id, confirmation_token, destination_identifier,
                    expected_content_hash, expected_baseline_hash, consumed,
                    expires_at, created_at, config_fingerprint
                ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                """,
                (
                    auth.transfer_id,
                    auth.confirmation_token,
                    auth.destination_identifier,
                    auth.expected_content_hash,
                    auth.expected_baseline_hash,
                    1 if auth.consumed else 0,
                    auth.expires_at.isoformat(),
                    auth.config_fingerprint,
                ),
            )
            auth.id = cursor.lastrowid
            await db.commit()
            return auth

    async def get_transfer_authorization(self, token: str) -> Optional[TransferAuthorization]:
        """Retrieve authorization by token."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM transfer_authorizations WHERE confirmation_token = ?",
                (token,),
            )
            row = await cursor.fetchone()
            if not row:
                return None
            return TransferAuthorization(
                id=row["id"],
                transfer_id=row["transfer_id"],
                confirmation_token=row["confirmation_token"],
                destination_identifier=row["destination_identifier"],
                expected_content_hash=row["expected_content_hash"],
                expected_baseline_hash=row["expected_baseline_hash"],
                consumed=bool(row["consumed"]),
                expires_at=datetime.fromisoformat(row["expires_at"]) if isinstance(row["expires_at"], str) else row["expires_at"],
                created_at=datetime.fromisoformat(row["created_at"]) if isinstance(row["created_at"], str) else row["created_at"],
                config_fingerprint=row["config_fingerprint"] if "config_fingerprint" in row.keys() else None,
            )

    async def verify_and_consume_authorization(
        self,
        transfer_id: str,
        token: str,
        content_hash: str,
        destination: str,
        expected_config_fingerprint: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Strictly verify and atomically consume a single-use authorization token.
        Ensures token is unconsumed, unexpired, bound to exact transfer_id, content hash, and destination.
        """
        auth = await self.get_transfer_authorization(token)
        if not auth:
            return False, "Invalid confirmation token"

        if auth.consumed:
            return False, "Confirmation token has already been consumed (single-use constraint)"

        if datetime.utcnow() > auth.expires_at:
            return False, "Confirmation token has expired"

        if auth.transfer_id != transfer_id:
            return False, f"Confirmation token bound to transfer {auth.transfer_id}, got {transfer_id}"

        if auth.expected_content_hash != content_hash:
            return False, f"Content hash mismatch: authorization bound to {auth.expected_content_hash[:8]}, got {content_hash[:8]}"

        # Authorization is bound to one exact target, never a substring/session prefix.
        if auth.destination_identifier != destination:
            return False, f"Destination mismatch: expected {auth.destination_identifier}, got {destination}"

        if expected_config_fingerprint is not None and auth.config_fingerprint:
            if auth.config_fingerprint != expected_config_fingerprint:
                return False, f"Configuration has changed since authorization was issued: bound to {auth.config_fingerprint[:8]}, current is {expected_config_fingerprint[:8]}"

        # Atomically consume
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "UPDATE transfer_authorizations SET consumed = 1 WHERE confirmation_token = ? AND consumed = 0",
                (token,),
            )
            await db.commit()
            if cursor.rowcount != 1:
                return False, "Confirmation token has already been consumed (single-use constraint)"

        return True, None

    async def invalidate_transfer_authorization(self, token_or_transfer_id: str) -> None:
        """Mark authorization consumed so it cannot be used."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE transfer_authorizations SET consumed = 1 WHERE confirmation_token = ? OR transfer_id = ?",
                (token_or_transfer_id, token_or_transfer_id),
            )
            await db.commit()

    async def persist_prepared_transfer_atomic(
        self,
        transfer: TransferRecord,
        auth: TransferAuthorization,
        checkpoint: CheckpointRecord,
        event_message: str,
    ) -> Tuple[TransferRecord, TransferAuthorization, CheckpointRecord]:
        """
        Atomically persist transfer record, single-use authorization,
        checkpoint, and transition event log in one database transaction.
        Enforces that only one active prepared transfer exists in this direction.
        If any step fails, all mutations are rolled back.
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                # 1. Enforce only one active prepared transfer in this direction
                cursor = await db.execute(
                    "SELECT COUNT(*) FROM transfers WHERE direction = ? AND status = 'prepared'",
                    (transfer.direction,)
                )
                row = await cursor.fetchone()
                if row and row[0] > 0:
                    raise ValueError(f"An active prepared transfer already exists for direction {transfer.direction}")

                # 2. Persist authorization
                auth_cursor = await db.execute(
                    """
                    INSERT INTO transfer_authorizations (
                        transfer_id, confirmation_token, destination_identifier,
                        expected_content_hash, expected_baseline_hash, consumed,
                        expires_at, created_at, config_fingerprint
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                    """,
                    (
                        auth.transfer_id,
                        auth.confirmation_token,
                        auth.destination_identifier,
                        auth.expected_content_hash,
                        auth.expected_baseline_hash,
                        1 if auth.consumed else 0,
                        auth.expires_at.isoformat() if hasattr(auth.expires_at, 'isoformat') else str(auth.expires_at),
                        auth.config_fingerprint,
                    ),
                )
                auth.id = auth_cursor.lastrowid

                # 3. Persist transfer
                transfer_cursor = await db.execute(
                    """
                    INSERT INTO transfers (
                        transfer_id, iteration_id, direction, source_type, source_identifier,
                        source_message_id, destination_type, destination_identifier, content_hash,
                        char_count, message_text, payload_preview, baseline_message_id,
                        baseline_hash, status, delivery_evidence, error_message, human_confirmed,
                        confirmation_token, is_operator_modified, provenance_policy, confirmed_at,
                        sent_at, verified_at, config_fingerprint, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (
                        transfer.transfer_id, transfer.iteration_id, transfer.direction, transfer.source_type,
                        transfer.source_identifier, transfer.source_message_id, transfer.destination_type,
                        transfer.destination_identifier, transfer.content_hash, transfer.char_count,
                        transfer.message_text, transfer.payload_preview, transfer.baseline_message_id,
                        transfer.baseline_hash, transfer.status,
                        json.dumps(transfer.delivery_evidence) if transfer.delivery_evidence else None,
                        transfer.error_message, 1 if transfer.human_confirmed else 0, transfer.confirmation_token,
                        1 if transfer.is_operator_modified else 0, transfer.provenance_policy,
                        transfer.confirmed_at.isoformat() if hasattr(transfer.confirmed_at, 'isoformat') and transfer.confirmed_at else None,
                        transfer.sent_at.isoformat() if hasattr(transfer.sent_at, 'isoformat') and transfer.sent_at else None,
                        transfer.verified_at.isoformat() if hasattr(transfer.verified_at, 'isoformat') and transfer.verified_at else None,
                        transfer.config_fingerprint,
                    ),
                )
                transfer.id = transfer_cursor.lastrowid

                # 4. Persist checkpoint
                cp_cursor = await db.execute(
                    """
                    INSERT INTO checkpoints (
                        stage, iteration_id, source_identifier, destination_identifier,
                        content_hash, payload_preview, transfer_status, failure_details, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        checkpoint.stage.value if hasattr(checkpoint.stage, 'value') else str(checkpoint.stage),
                        checkpoint.iteration_id,
                        checkpoint.source_identifier,
                        checkpoint.destination_identifier,
                        checkpoint.content_hash,
                        checkpoint.payload_preview,
                        checkpoint.transfer_status,
                        checkpoint.failure_details,
                    ),
                )
                checkpoint.id = cp_cursor.lastrowid

                # 5. Persist event log
                await db.execute(
                    """
                    INSERT INTO event_logs (event_type, severity, source, stage, message, details)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "STAGE_TRANSITION",
                        "info",
                        "system",
                        checkpoint.stage.value if hasattr(checkpoint.stage, 'value') else str(checkpoint.stage),
                        event_message,
                        json.dumps({"iteration": checkpoint.iteration_id, "transfer_status": checkpoint.transfer_status, "transfer_id": transfer.transfer_id}),
                    ),
                )

                await db.commit()
                return transfer, auth, checkpoint
            except Exception:
                await db.rollback()
                raise


    async def save_canary_round(self, round_data: Dict[str, Any]) -> Dict[str, Any]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            metrics_json = json.dumps(round_data.get("metrics")) if round_data.get("metrics") else None
            started_at = round_data.get("started_at")
            if isinstance(started_at, datetime):
                started_at = started_at.isoformat()
            completed_at = round_data.get("completed_at")
            if isinstance(completed_at, datetime):
                completed_at = completed_at.isoformat()

            recovered_at = round_data.get("recovered_at")
            if isinstance(recovered_at, datetime):
                recovered_at = recovered_at.isoformat()

            await db.execute(
                """
                INSERT INTO canary_rounds (
                    round_id, round_number, nonce, c2g_transfer_id,
                    chatgpt_user_turn_id, chatgpt_assistant_turn_id,
                    g2c_transfer_id, codex_queue_receipt_id,
                    codex_user_turn_id, codex_assistant_message_id,
                    started_at, completed_at, result,
                    manual_thread_load_required, metrics, error_details,
                    is_recovered, recovery_notes, recovered_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(round_id) DO UPDATE SET
                    round_number = excluded.round_number,
                    nonce = excluded.nonce,
                    c2g_transfer_id = COALESCE(excluded.c2g_transfer_id, canary_rounds.c2g_transfer_id),
                    chatgpt_user_turn_id = COALESCE(excluded.chatgpt_user_turn_id, canary_rounds.chatgpt_user_turn_id),
                    chatgpt_assistant_turn_id = COALESCE(excluded.chatgpt_assistant_turn_id, canary_rounds.chatgpt_assistant_turn_id),
                    g2c_transfer_id = COALESCE(excluded.g2c_transfer_id, canary_rounds.g2c_transfer_id),
                    codex_queue_receipt_id = COALESCE(excluded.codex_queue_receipt_id, canary_rounds.codex_queue_receipt_id),
                    codex_user_turn_id = COALESCE(excluded.codex_user_turn_id, canary_rounds.codex_user_turn_id),
                    codex_assistant_message_id = COALESCE(excluded.codex_assistant_message_id, canary_rounds.codex_assistant_message_id),
                    completed_at = COALESCE(excluded.completed_at, canary_rounds.completed_at),
                    result = excluded.result,
                    manual_thread_load_required = excluded.manual_thread_load_required,
                    metrics = COALESCE(excluded.metrics, canary_rounds.metrics),
                    error_details = COALESCE(excluded.error_details, canary_rounds.error_details),
                    is_recovered = COALESCE(excluded.is_recovered, canary_rounds.is_recovered),
                    recovery_notes = COALESCE(excluded.recovery_notes, canary_rounds.recovery_notes),
                    recovered_at = COALESCE(excluded.recovered_at, canary_rounds.recovered_at)
                """,
                (
                    round_data["round_id"],
                    round_data.get("round_number", 1),
                    round_data["nonce"],
                    round_data.get("c2g_transfer_id"),
                    round_data.get("chatgpt_user_turn_id"),
                    round_data.get("chatgpt_assistant_turn_id"),
                    round_data.get("g2c_transfer_id"),
                    round_data.get("codex_queue_receipt_id"),
                    round_data.get("codex_user_turn_id"),
                    round_data.get("codex_assistant_message_id"),
                    started_at,
                    completed_at,
                    round_data.get("result", "ROUND_COMPLETED"),
                    1 if round_data.get("manual_thread_load_required") else 0,
                    metrics_json,
                    round_data.get("error_details"),
                    1 if round_data.get("is_recovered") else 0,
                    round_data.get("recovery_notes"),
                    recovered_at,
                ),
            )
            await db.commit()
            return await self.get_canary_round(round_data["round_id"])

    async def update_canary_round(self, round_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        existing = await self.get_canary_round(round_id)
        if not existing:
            return None
        existing.update(updates)
        return await self.save_canary_round(existing)

    async def get_canary_round(self, round_id: str) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM canary_rounds WHERE round_id = ?", (round_id,))
            row = await cursor.fetchone()
            if not row:
                return None
            item = dict(row)
            if item.get("metrics") and isinstance(item["metrics"], str):
                try:
                    item["metrics"] = json.loads(item["metrics"])
                except Exception:
                    pass
            item["manual_thread_load_required"] = bool(item.get("manual_thread_load_required"))
            return item

    async def get_canary_rounds(self) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM canary_rounds ORDER BY round_number ASC, id ASC")
            rows = await cursor.fetchall()
            results = []
            for r in rows:
                item = dict(r)
                if item.get("metrics") and isinstance(item["metrics"], str):
                    try:
                        item["metrics"] = json.loads(item["metrics"])
                    except Exception:
                        pass
                item["manual_thread_load_required"] = bool(item.get("manual_thread_load_required"))
                results.append(item)
            return results

    async def get_latest_canary_round(self) -> Optional[Dict[str, Any]]:
        rounds = await self.get_canary_rounds()
        return rounds[-1] if rounds else None
