from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from overlord.models import (
    ACTIVE_PHASES,
    ALLOWED_TRANSITIONS,
    PHASE_ORDER,
    ConflictRecord,
    DashboardSnapshot,
    WorkerDetail,
    WorkerEventCreate,
    WorkerEventRecord,
    WorkerNoteCreate,
    WorkerNoteRecord,
    WorkerPhase,
    WorkerSummary,
)


class InvalidTransitionError(ValueError):
    pass


class WorkerAuthError(PermissionError):
    pass


def _connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


class StateStore:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "overlord.db"
        self._initialize()

    def _initialize(self) -> None:
        with _connect(self.db_path) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
                    worker_token TEXT NOT NULL,
                    repo_path TEXT NOT NULL,
                    branch TEXT,
                    worktree TEXT,
                    owned_artifact TEXT,
                    current_phase TEXT NOT NULL,
                    status_line TEXT NOT NULL,
                    next_irreversible_step TEXT,
                    blocker TEXT,
                    pr_url TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS phase_transitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    worker_id TEXT NOT NULL,
                    previous_phase TEXT,
                    current_phase TEXT NOT NULL,
                    status_line TEXT NOT NULL,
                    next_irreversible_step TEXT,
                    blocker TEXT,
                    note TEXT,
                    repo_path TEXT NOT NULL,
                    branch TEXT,
                    worktree TEXT,
                    owned_artifact TEXT,
                    pr_url TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(worker_id) REFERENCES workers(worker_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS phase_notes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    worker_id TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    note TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(worker_id) REFERENCES workers(worker_id) ON DELETE CASCADE
                );
                """
            )

    def record_event(self, event: WorkerEventCreate) -> WorkerDetail:
        with _connect(self.db_path) as connection:
            current = connection.execute(
                "SELECT * FROM workers WHERE worker_id = ?",
                (event.worker_id,),
            ).fetchone()

            self._validate_worker_auth(current, event.worker_token)
            self._validate_transition(current, event)

            if current is None:
                connection.execute(
                    """
                    INSERT INTO workers (
                        worker_id, worker_token, repo_path, branch, worktree, owned_artifact,
                        current_phase, status_line, next_irreversible_step, blocker, pr_url, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.worker_id,
                        event.worker_token,
                        event.repo_path,
                        event.branch,
                        event.worktree,
                        event.owned_artifact,
                        event.current_phase.value,
                        event.status_line,
                        event.next_irreversible_step,
                        event.blocker,
                        str(event.pr_url) if event.pr_url else None,
                        event.timestamp.isoformat(),
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE workers
                    SET repo_path = ?, branch = ?, worktree = ?, owned_artifact = ?,
                        current_phase = ?, status_line = ?, next_irreversible_step = ?,
                        blocker = ?, pr_url = ?, updated_at = ?
                    WHERE worker_id = ?
                    """,
                    (
                        event.repo_path,
                        event.branch,
                        event.worktree,
                        event.owned_artifact,
                        event.current_phase.value,
                        event.status_line,
                        event.next_irreversible_step,
                        event.blocker,
                        str(event.pr_url) if event.pr_url else None,
                        event.timestamp.isoformat(),
                        event.worker_id,
                    ),
                )

            connection.execute(
                """
                INSERT INTO phase_transitions (
                    worker_id, previous_phase, current_phase, status_line, next_irreversible_step,
                    blocker, note, repo_path, branch, worktree, owned_artifact, pr_url, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.worker_id,
                    event.previous_phase.value if event.previous_phase else None,
                    event.current_phase.value,
                    event.status_line,
                    event.next_irreversible_step,
                    event.blocker,
                    event.note,
                    event.repo_path,
                    event.branch,
                    event.worktree,
                    event.owned_artifact,
                    str(event.pr_url) if event.pr_url else None,
                    event.timestamp.isoformat(),
                ),
            )

            if event.note:
                connection.execute(
                    """
                    INSERT INTO phase_notes (worker_id, phase, note, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        event.worker_id,
                        event.current_phase.value,
                        event.note,
                        event.timestamp.isoformat(),
                    ),
                )

        return self.get_worker(event.worker_id)

    def add_note(self, worker_id: str, note: WorkerNoteCreate) -> WorkerNoteRecord:
        with _connect(self.db_path) as connection:
            current = connection.execute(
                "SELECT * FROM workers WHERE worker_id = ?",
                (worker_id,),
            ).fetchone()
            if current is None:
                raise KeyError(worker_id)
            self._validate_worker_auth(current, note.worker_token)

            cursor = connection.execute(
                """
                INSERT INTO phase_notes (worker_id, phase, note, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    worker_id,
                    note.phase.value,
                    note.note,
                    note.timestamp.isoformat(),
                ),
            )

        return WorkerNoteRecord(
            id=cursor.lastrowid,
            worker_id=worker_id,
            phase=note.phase,
            note=note.note,
            created_at=note.timestamp,
        )

    def get_worker(self, worker_id: str) -> WorkerDetail:
        with _connect(self.db_path) as connection:
            current = connection.execute(
                "SELECT * FROM workers WHERE worker_id = ?",
                (worker_id,),
            ).fetchone()
            if current is None:
                raise KeyError(worker_id)

            transitions = connection.execute(
                """
                SELECT * FROM phase_transitions
                WHERE worker_id = ?
                ORDER BY datetime(created_at) DESC, id DESC
                """,
                (worker_id,),
            ).fetchall()
            notes = connection.execute(
                """
                SELECT * FROM phase_notes
                WHERE worker_id = ?
                ORDER BY datetime(created_at) DESC, id DESC
                """,
                (worker_id,),
            ).fetchall()

        summary = self._row_to_summary(current, self._row_to_note(notes[0]) if notes else None)
        return WorkerDetail(
            **summary.model_dump(),
            transitions=[self._row_to_transition(row) for row in transitions],
            notes=[self._row_to_note(row) for row in notes],
        )

    def snapshot(self) -> DashboardSnapshot:
        with _connect(self.db_path) as connection:
            workers = connection.execute(
                """
                SELECT * FROM workers
                ORDER BY
                    CASE current_phase
                        WHEN 'assigned' THEN 0
                        WHEN 'scouting' THEN 1
                        WHEN 'planned' THEN 2
                        WHEN 'implementing' THEN 3
                        WHEN 'validating' THEN 4
                        WHEN 'blocked' THEN 5
                        WHEN 'handoff-ready' THEN 6
                        ELSE 7
                    END,
                    datetime(updated_at) DESC,
                    worker_id ASC
                """
            ).fetchall()
            recent_notes = connection.execute(
                """
                SELECT * FROM phase_notes
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT 12
                """
            ).fetchall()
            latest_note_rows = connection.execute(
                """
                SELECT pn.*
                FROM phase_notes pn
                JOIN (
                    SELECT worker_id, MAX(id) AS latest_id
                    FROM phase_notes
                    GROUP BY worker_id
                ) latest ON latest.latest_id = pn.id
                """
            ).fetchall()

        latest_notes = {
            row["worker_id"]: self._row_to_note(row)
            for row in latest_note_rows
        }
        summaries = [self._row_to_summary(row, latest_notes.get(row["worker_id"])) for row in workers]

        by_phase = {
            phase: [worker for worker in summaries if worker.phase == phase]
            for phase in PHASE_ORDER
        }
        totals = {
            "workers": len(summaries),
            "active": sum(1 for worker in summaries if worker.phase in ACTIVE_PHASES),
            "blocked": len(by_phase[WorkerPhase.BLOCKED]),
            "handoff_ready": len(by_phase[WorkerPhase.HANDOFF_READY]),
        }

        return DashboardSnapshot(
            workers=summaries,
            by_phase=by_phase,
            conflicts=self._detect_conflicts(summaries),
            recent_notes=[self._row_to_note(row) for row in recent_notes],
            totals=totals,
        )

    def _validate_worker_auth(self, current: sqlite3.Row | None, worker_token: str) -> None:
        if current is not None and current["worker_token"] != worker_token:
            raise WorkerAuthError("worker token does not match existing worker")

    def _validate_transition(self, current: sqlite3.Row | None, event: WorkerEventCreate) -> None:
        stored_phase = WorkerPhase(current["current_phase"]) if current is not None else None
        if event.previous_phase != stored_phase:
            raise InvalidTransitionError(
                f"previous_phase must match stored phase {stored_phase.value if stored_phase else 'none'}"
            )
        allowed = ALLOWED_TRANSITIONS[stored_phase]
        if event.current_phase not in allowed:
            raise InvalidTransitionError(
                f"transition from {stored_phase.value if stored_phase else 'none'} "
                f"to {event.current_phase.value} is not allowed"
            )

    def _detect_conflicts(self, workers: list[WorkerSummary]) -> list[ConflictRecord]:
        buckets: dict[tuple[str, str], set[str]] = defaultdict(set)
        for worker in workers:
            if worker.phase not in ACTIVE_PHASES:
                continue
            for field in ("repo_path", "branch", "worktree", "owned_artifact"):
                value = getattr(worker, field)
                if value:
                    buckets[(field, value)].add(worker.worker_id)

        conflicts: list[ConflictRecord] = []
        for (field, value), worker_ids in sorted(buckets.items()):
            if len(worker_ids) > 1:
                conflicts.append(
                    ConflictRecord(
                        field=field,
                        value=value,
                        worker_ids=sorted(worker_ids),
                    )
                )
        return conflicts

    def _row_to_summary(self, row: sqlite3.Row, last_note: WorkerNoteRecord | None) -> WorkerSummary:
        return WorkerSummary(
            worker_id=row["worker_id"],
            phase=WorkerPhase(row["current_phase"]),
            status_line=row["status_line"],
            repo_path=row["repo_path"],
            branch=row["branch"],
            worktree=row["worktree"],
            owned_artifact=row["owned_artifact"],
            next_irreversible_step=row["next_irreversible_step"],
            blocker=row["blocker"],
            pr_url=row["pr_url"],
            updated_at=self._parse_timestamp(row["updated_at"]),
            last_note=last_note,
        )

    def _row_to_transition(self, row: sqlite3.Row) -> WorkerEventRecord:
        return WorkerEventRecord(
            id=row["id"],
            worker_id=row["worker_id"],
            previous_phase=WorkerPhase(row["previous_phase"]) if row["previous_phase"] else None,
            current_phase=WorkerPhase(row["current_phase"]),
            status_line=row["status_line"],
            next_irreversible_step=row["next_irreversible_step"],
            blocker=row["blocker"],
            note=row["note"],
            repo_path=row["repo_path"],
            branch=row["branch"],
            worktree=row["worktree"],
            owned_artifact=row["owned_artifact"],
            pr_url=row["pr_url"],
            created_at=self._parse_timestamp(row["created_at"]),
        )

    def _row_to_note(self, row: sqlite3.Row) -> WorkerNoteRecord:
        return WorkerNoteRecord(
            id=row["id"],
            worker_id=row["worker_id"],
            phase=WorkerPhase(row["phase"]),
            note=row["note"],
            created_at=self._parse_timestamp(row["created_at"]),
        )

    def _parse_timestamp(self, value: str) -> datetime:
        return datetime.fromisoformat(value)
