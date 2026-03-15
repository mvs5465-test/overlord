from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


class WorkerStatus(str, Enum):
    assigned = "assigned"
    scouting = "scouting"
    planned = "planned"
    implementing = "implementing"
    validating = "validating"
    blocked = "blocked"
    handoff_ready = "handoff-ready"
    terminal = "terminal"


class WorkerStatusEventIn(BaseModel):
    worker_id: str = Field(alias="workerId", min_length=1, max_length=120)
    status: WorkerStatus
    previous_status: WorkerStatus | None = Field(default=None, alias="previousStatus")
    repo_path: str = Field(alias="repoPath", min_length=1, max_length=4096)
    branch: str | None = Field(default=None, max_length=255)
    artifact: str | None = Field(default=None, max_length=255)
    note: str | None = Field(default=None, max_length=280)
    next_step: str | None = Field(default=None, alias="nextStep", max_length=280)

    @field_validator("worker_id", "repo_path", "branch", "artifact", "note", "next_step")
    @classmethod
    def strip_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None


class WorkerStatusEventRecord(BaseModel):
    event_id: str = Field(alias="eventId")
    accepted_at: str = Field(alias="acceptedAt")
    worker_id: str = Field(alias="workerId")
    status: WorkerStatus
    previous_status: WorkerStatus | None = Field(alias="previousStatus", default=None)
    repo_path: str = Field(alias="repoPath")
    branch: str | None = None
    artifact: str | None = None
    note: str | None = None
    next_step: str | None = Field(alias="nextStep", default=None)


class WorkerEventStore:
    def __init__(self, data_dir: Path) -> None:
        self._path = data_dir / "worker-status-events.jsonl"

    def append_status_event(self, event: WorkerStatusEventIn) -> WorkerStatusEventRecord:
        record = WorkerStatusEventRecord(
            eventId=str(uuid4()),
            acceptedAt=datetime.now(timezone.utc).isoformat(),
            workerId=event.worker_id,
            status=event.status,
            previousStatus=event.previous_status,
            repoPath=event.repo_path,
            branch=event.branch,
            artifact=event.artifact,
            note=event.note,
            nextStep=event.next_step,
        )
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as fh:
            fh.write(record.model_dump_json(by_alias=True))
            fh.write("\n")
        return record
