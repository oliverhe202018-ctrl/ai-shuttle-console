"""
State Machine Engine for Shuttle Orchestration.
Guarantees typed transitions, monotonic check-pointing, duplicate prevention, and unknown delivery handling.
"""
from typing import Optional, Tuple
from app.database import Database
from app.models import (
    AppConfig,
    CheckpointRecord,
    ShuttleStage,
    VALID_TRANSITIONS,
    compute_sha256,
)


class InvalidTransitionError(Exception):
    pass


class ShuttleStateMachine:
    def __init__(self, db: Database, config: Optional[AppConfig] = None):
        self.db = db
        self.config = config
        self.current_checkpoint: Optional[CheckpointRecord] = None

    async def init(self):
        """Restore latest checkpoint from DB or initialize to IDLE."""
        latest = await self.db.get_latest_checkpoint()
        if latest:
            self.current_checkpoint = latest
        else:
            init_cp = CheckpointRecord(
                stage=ShuttleStage.IDLE,
                iteration_id=0,
                source_identifier="init",
                destination_identifier="init",
                transfer_status="idle",
            )
            self.current_checkpoint = await self.db.save_checkpoint(init_cp)
            await self.db.log_event(
                event_type="SHUTTLE_INIT",
                stage=ShuttleStage.IDLE,
                message="Shuttle state machine initialized at IDLE stage",
            )

    @property
    def stage(self) -> ShuttleStage:
        return self.current_checkpoint.stage if self.current_checkpoint else ShuttleStage.IDLE

    def can_transition_to(self, target_stage: ShuttleStage) -> bool:
        current = self.stage
        if current == target_stage:
            return True
        allowed = VALID_TRANSITIONS.get(current, [])
        return target_stage in allowed

    async def transition_to(
        self,
        target_stage: ShuttleStage,
        iteration_id: Optional[int] = None,
        source_id: Optional[str] = None,
        dest_id: Optional[str] = None,
        content_hash: Optional[str] = None,
        payload_preview: Optional[str] = None,
        transfer_status: Optional[str] = None,
        failure_details: Optional[str] = None,
        force: bool = False,
    ) -> CheckpointRecord:
        """
        Atomically validate and persist transition before side-effects.
        """
        if not force and not self.can_transition_to(target_stage):
            msg = f"Invalid state transition requested: {self.stage} -> {target_stage}"
            await self.db.log_event(
                event_type="TRANSITION_REJECTED",
                stage=self.stage,
                severity="warning",
                message=msg,
            )
            raise InvalidTransitionError(msg)

        cur = self.current_checkpoint
        new_record = CheckpointRecord(
            stage=target_stage,
            iteration_id=iteration_id if iteration_id is not None else (cur.iteration_id if cur else 0),
            source_identifier=source_id if source_id is not None else (cur.source_identifier if cur else ""),
            destination_identifier=dest_id if dest_id is not None else (cur.destination_identifier if cur else ""),
            content_hash=content_hash if content_hash is not None else (cur.content_hash if cur else None),
            payload_preview=payload_preview if payload_preview is not None else (cur.payload_preview if cur else None),
            transfer_status=transfer_status if transfer_status is not None else (cur.transfer_status if cur else "pending"),
            failure_details=failure_details if failure_details is not None else (cur.failure_details if cur else None),
        )

        saved = await self.db.save_checkpoint(new_record)
        self.current_checkpoint = saved

        await self.db.log_event(
            event_type="STAGE_TRANSITION",
            stage=target_stage,
            message=f"Transitioned from {cur.stage if cur else 'NONE'} to {target_stage}",
            details={"iteration": saved.iteration_id, "transfer_status": saved.transfer_status},
        )
        return saved

    async def check_duplicate(self, content: str) -> Tuple[bool, Optional[str]]:
        """
        Verify if exact report content or hash has already been transferred to ChatGPT.
        """
        content_hash = compute_sha256(content)
        existing = await self.db.find_transfer_by_hash(content_hash)
        for t in existing:
            if t.get("status") in ("sent", "confirmed"):
                return True, f"Duplicate detected: transfer ID {t['id']} already sent with hash {content_hash[:8]}"
        return False, None
