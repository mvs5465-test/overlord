from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class WorkerPhase(StrEnum):
    ASSIGNED = "assigned"
    SCOUTING = "scouting"
    PLANNED = "planned"
    IMPLEMENTING = "implementing"
    VALIDATING = "validating"
    BLOCKED = "blocked"
    HANDOFF_READY = "handoff-ready"
    TERMINAL = "terminal"


ACTIVE_PHASES = {
    WorkerPhase.ASSIGNED,
    WorkerPhase.SCOUTING,
    WorkerPhase.PLANNED,
    WorkerPhase.IMPLEMENTING,
    WorkerPhase.VALIDATING,
    WorkerPhase.BLOCKED,
    WorkerPhase.HANDOFF_READY,
}

ALLOWED_TRANSITIONS: dict[WorkerPhase | None, set[WorkerPhase]] = {
    None: {WorkerPhase.ASSIGNED},
    WorkerPhase.ASSIGNED: {WorkerPhase.SCOUTING, WorkerPhase.BLOCKED, WorkerPhase.TERMINAL},
    WorkerPhase.SCOUTING: {
        WorkerPhase.PLANNED,
        WorkerPhase.IMPLEMENTING,
        WorkerPhase.BLOCKED,
        WorkerPhase.TERMINAL,
    },
    WorkerPhase.PLANNED: {
        WorkerPhase.IMPLEMENTING,
        WorkerPhase.BLOCKED,
        WorkerPhase.TERMINAL,
    },
    WorkerPhase.IMPLEMENTING: {
        WorkerPhase.VALIDATING,
        WorkerPhase.BLOCKED,
        WorkerPhase.TERMINAL,
    },
    WorkerPhase.VALIDATING: {
        WorkerPhase.HANDOFF_READY,
        WorkerPhase.BLOCKED,
        WorkerPhase.TERMINAL,
    },
    WorkerPhase.BLOCKED: {
        WorkerPhase.SCOUTING,
        WorkerPhase.PLANNED,
        WorkerPhase.IMPLEMENTING,
        WorkerPhase.VALIDATING,
        WorkerPhase.HANDOFF_READY,
        WorkerPhase.TERMINAL,
    },
    WorkerPhase.HANDOFF_READY: {WorkerPhase.TERMINAL, WorkerPhase.BLOCKED},
    WorkerPhase.TERMINAL: set(),
}

PHASE_ORDER = [
    WorkerPhase.ASSIGNED,
    WorkerPhase.SCOUTING,
    WorkerPhase.PLANNED,
    WorkerPhase.IMPLEMENTING,
    WorkerPhase.VALIDATING,
    WorkerPhase.BLOCKED,
    WorkerPhase.HANDOFF_READY,
    WorkerPhase.TERMINAL,
]


class WorkerEventCreate(BaseModel):
    worker_id: str = Field(min_length=3, max_length=120)
    worker_token: str = Field(min_length=8, max_length=200)
    current_phase: WorkerPhase
    previous_phase: WorkerPhase | None = None
    repo_path: str = Field(min_length=1, max_length=500)
    branch: str | None = Field(default=None, max_length=255)
    worktree: str | None = Field(default=None, max_length=500)
    owned_artifact: str | None = Field(default=None, max_length=500)
    status_line: str = Field(min_length=1, max_length=240)
    next_irreversible_step: str | None = Field(default=None, max_length=240)
    blocker: str | None = Field(default=None, max_length=240)
    note: str | None = Field(default=None, max_length=500)
    pr_url: HttpUrl | None = None
    timestamp: datetime = Field(default_factory=utc_now)

    @field_validator("timestamp")
    @classmethod
    def ensure_timestamp_has_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_phase_requirements(self) -> "WorkerEventCreate":
        if self.current_phase == WorkerPhase.BLOCKED and not self.blocker:
            raise ValueError("blocked transitions must include a blocker")
        if self.current_phase in {
            WorkerPhase.PLANNED,
            WorkerPhase.IMPLEMENTING,
            WorkerPhase.VALIDATING,
        } and not self.next_irreversible_step:
            raise ValueError("this phase requires a next_irreversible_step")
        if self.previous_phase == self.current_phase:
            raise ValueError("previous_phase must differ from current_phase")
        return self


class WorkerNoteCreate(BaseModel):
    worker_token: str = Field(min_length=8, max_length=200)
    phase: WorkerPhase
    note: str = Field(min_length=1, max_length=500)
    timestamp: datetime = Field(default_factory=utc_now)

    @field_validator("timestamp")
    @classmethod
    def ensure_timestamp_has_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class WorkerNoteRecord(BaseModel):
    id: int
    worker_id: str
    phase: WorkerPhase
    note: str
    created_at: datetime


class WorkerEventRecord(BaseModel):
    id: int
    worker_id: str
    previous_phase: WorkerPhase | None
    current_phase: WorkerPhase
    status_line: str
    next_irreversible_step: str | None
    blocker: str | None
    note: str | None
    repo_path: str
    branch: str | None
    worktree: str | None
    owned_artifact: str | None
    pr_url: str | None
    created_at: datetime


class WorkerSummary(BaseModel):
    worker_id: str
    phase: WorkerPhase
    status_line: str
    repo_path: str
    branch: str | None
    worktree: str | None
    owned_artifact: str | None
    next_irreversible_step: str | None
    blocker: str | None
    pr_url: str | None
    updated_at: datetime
    last_note: WorkerNoteRecord | None = None


class WorkerDetail(WorkerSummary):
    transitions: list[WorkerEventRecord]
    notes: list[WorkerNoteRecord]


class ConflictRecord(BaseModel):
    field: str
    value: str
    worker_ids: list[str]


class DashboardSnapshot(BaseModel):
    workers: list[WorkerSummary]
    by_phase: dict[WorkerPhase, list[WorkerSummary]]
    conflicts: list[ConflictRecord]
    recent_notes: list[WorkerNoteRecord]
    totals: dict[str, int]


class DispatchStatus(StrEnum):
    LAUNCHED = "launched"


class OperatorCommandCreate(BaseModel):
    general_worker_id: str = Field(min_length=3, max_length=120)
    repo_path: str = Field(min_length=1, max_length=500)
    branch_hint: str | None = Field(default=None, max_length=255)
    operator_instruction: str = Field(min_length=1, max_length=4000)
    created_at: datetime = Field(default_factory=utc_now)

    @field_validator("created_at")
    @classmethod
    def ensure_created_at_has_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator("repo_path")
    @classmethod
    def normalize_repo_path(cls, value: str) -> str:
        return str(Path(value).expanduser().resolve())


class OperatorCommandLaunch(BaseModel):
    status: DispatchStatus
    pid: int
    prompt_path: str
    log_path: str


class OperatorCommandRecord(BaseModel):
    id: int
    general_worker_id: str
    repo_path: str
    branch_hint: str | None
    operator_instruction: str
    status: DispatchStatus
    pid: int
    prompt_path: str
    log_path: str
    created_at: datetime
