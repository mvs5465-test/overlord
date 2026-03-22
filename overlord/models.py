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


class MemberRole(StrEnum):
    GENERAL = "general"
    CAPTAIN = "captain"
    WORKER = "worker"


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
    role: MemberRole = MemberRole.WORKER
    parent_worker_id: str | None = Field(default=None, min_length=3, max_length=120)
    current_phase: WorkerPhase
    previous_phase: WorkerPhase | None = None
    repo_path: str = Field(min_length=1, max_length=500)
    branch: str | None = Field(default=None, max_length=255)
    worktree: str | None = Field(default=None, max_length=500)
    owned_artifact: str | None = Field(default=None, max_length=500)
    host_id: str | None = Field(default="localhost", max_length=255)
    process_id: int | None = Field(default=None, ge=1)
    process_started_at: datetime | None = None
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
        if self.parent_worker_id == self.worker_id:
            raise ValueError("parent_worker_id must differ from worker_id")
        if self.role == MemberRole.GENERAL and self.parent_worker_id is not None:
            raise ValueError("general members may not declare a parent_worker_id")
        return self

    @field_validator("process_started_at")
    @classmethod
    def ensure_process_started_at_has_timezone(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


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
    role: MemberRole = MemberRole.WORKER
    parent_worker_id: str | None = None
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
    host_id: str | None = None
    process_id: int | None = None
    process_started_at: datetime | None = None
    pr_url: str | None
    created_at: datetime


class RegistrationCreate(BaseModel):
    member_id: str = Field(min_length=3, max_length=120)
    member_token: str = Field(min_length=8, max_length=200)
    role: MemberRole
    parent_member_id: str | None = Field(default=None, min_length=3, max_length=120)
    parent_token: str | None = Field(default=None, min_length=8, max_length=200)
    repo_path: str = Field(min_length=1, max_length=500)
    branch: str | None = Field(default=None, max_length=255)
    worktree: str | None = Field(default=None, max_length=500)
    owned_artifact: str | None = Field(default=None, max_length=500)
    host_id: str | None = Field(default="localhost", max_length=255)
    process_id: int | None = Field(default=None, ge=1)
    process_started_at: datetime | None = None
    phase: WorkerPhase = WorkerPhase.ASSIGNED
    status_line: str = Field(min_length=1, max_length=240)
    note: str | None = Field(default=None, max_length=500)
    timestamp: datetime = Field(default_factory=utc_now)

    @field_validator("timestamp", "process_started_at")
    @classmethod
    def ensure_registration_timestamps_have_timezone(
        cls,
        value: datetime | None,
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_lineage_requirements(self) -> "RegistrationCreate":
        if self.parent_member_id == self.member_id:
            raise ValueError("parent_member_id must differ from member_id")
        if self.role == MemberRole.GENERAL and self.parent_member_id is not None:
            raise ValueError("general members may not declare a parent_member_id")
        return self


class RegistrationRecord(BaseModel):
    id: int
    member_id: str
    role: MemberRole
    parent_member_id: str | None
    repo_path: str
    branch: str | None
    worktree: str | None
    owned_artifact: str | None
    host_id: str | None
    process_id: int | None
    process_started_at: datetime | None
    phase: WorkerPhase
    status_line: str
    note: str | None
    created_at: datetime


class ParentReportCreate(BaseModel):
    subject_member_id: str = Field(min_length=3, max_length=120)
    reporter_member_id: str = Field(min_length=3, max_length=120)
    reporter_token: str = Field(min_length=8, max_length=200)
    event_type: str | None = Field(default=None, max_length=120)
    related_member_id: str | None = Field(default=None, min_length=3, max_length=120)
    observed_phase: WorkerPhase | None = None
    observed_status_line: str = Field(min_length=1, max_length=240)
    observed_state: str | None = Field(default=None, max_length=120)
    blocker: str | None = Field(default=None, max_length=240)
    note: str | None = Field(default=None, max_length=500)
    process_id: int | None = Field(default=None, ge=1)
    timestamp: datetime = Field(default_factory=utc_now)

    @field_validator("timestamp")
    @classmethod
    def ensure_parent_report_timestamp_has_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class ParentReportRecord(BaseModel):
    id: int
    subject_member_id: str
    reporter_member_id: str
    event_type: str | None = None
    related_member_id: str | None = None
    observed_phase: WorkerPhase | None
    observed_status_line: str
    observed_state: str | None
    blocker: str | None
    note: str | None
    process_id: int | None
    created_at: datetime


class MemberMessageCreate(BaseModel):
    member_id: str = Field(min_length=3, max_length=120)
    sender_member_id: str = Field(min_length=3, max_length=120)
    sender_token: str = Field(min_length=8, max_length=200)
    message_type: str = Field(min_length=3, max_length=120)
    body: str = Field(min_length=1, max_length=1000)
    related_member_id: str | None = Field(default=None, min_length=3, max_length=120)
    timestamp: datetime = Field(default_factory=utc_now)

    @field_validator("timestamp")
    @classmethod
    def ensure_message_timestamp_has_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class MemberMessageRecord(BaseModel):
    id: int
    member_id: str
    sender_member_id: str
    sender_role: MemberRole
    message_type: str
    body: str
    related_member_id: str | None = None
    created_at: datetime


class HeartbeatRecord(BaseModel):
    id: int
    member_id: str
    host_id: str | None
    process_id: int | None
    process_started_at: datetime | None
    observed_alive: bool
    observed_state: str
    created_at: datetime


class WorkerSummary(BaseModel):
    worker_id: str
    role: MemberRole = MemberRole.WORKER
    parent_worker_id: str | None = None
    phase: WorkerPhase
    status_line: str
    repo_path: str
    branch: str | None
    worktree: str | None
    owned_artifact: str | None
    host_id: str | None = None
    process_id: int | None = None
    process_started_at: datetime | None = None
    next_irreversible_step: str | None
    blocker: str | None
    pr_url: str | None
    updated_at: datetime
    registered_at: datetime | None = None
    last_self_reported_at: datetime | None = None
    last_parent_report: ParentReportRecord | None = None
    last_heartbeat: HeartbeatRecord | None = None
    effective_state: str = "unknown"
    last_note: WorkerNoteRecord | None = None
    last_message: MemberMessageRecord | None = None


class WorkerDetail(WorkerSummary):
    registrations: list[RegistrationRecord] = []
    parent_reports: list[ParentReportRecord] = []
    heartbeats: list[HeartbeatRecord] = []
    messages: list[MemberMessageRecord] = []
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


class DispatchRole(StrEnum):
    GENERAL = "general"
    CAPTAIN = "captain"


class OperatorCommandCreate(BaseModel):
    general_worker_id: str = Field(min_length=3, max_length=120)
    dispatch_role: DispatchRole = DispatchRole.GENERAL
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

    @model_validator(mode="after")
    def validate_dispatch_role(self) -> "OperatorCommandCreate":
        if self.dispatch_role not in {DispatchRole.GENERAL, DispatchRole.CAPTAIN}:
            raise ValueError("dispatch_role must be general or captain")
        return self


class OperatorCommandLaunch(BaseModel):
    status: DispatchStatus
    pid: int
    prompt_path: str
    log_path: str


class OperatorCommandRecord(BaseModel):
    id: int
    general_worker_id: str
    dispatch_role: DispatchRole
    repo_path: str
    branch_hint: str | None
    operator_instruction: str
    status: DispatchStatus
    pid: int
    prompt_path: str
    log_path: str
    created_at: datetime
