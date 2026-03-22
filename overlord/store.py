from __future__ import annotations

import logging
import os
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from overlord.models import (
    ACTIVE_PHASES,
    ALLOWED_TRANSITIONS,
    PHASE_ORDER,
    ConflictRecord,
    DashboardSnapshot,
    DispatchRole,
    HeartbeatRecord,
    MemberMessageCreate,
    MemberMessageRecord,
    MemberRole,
    OperatorCommandCreate,
    OperatorCommandLaunch,
    OperatorCommandRecord,
    ParentReportCreate,
    ParentReportRecord,
    RegistrationCreate,
    RegistrationRecord,
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


logger = logging.getLogger("overlord.store")


def _log_event(event: str, **fields: object) -> None:
    ordered = " ".join(f"{key}={fields[key]!r}" for key in sorted(fields))
    logger.info("event=%s %s", event, ordered)


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
                    role TEXT NOT NULL DEFAULT 'worker',
                    parent_worker_id TEXT,
                    repo_path TEXT NOT NULL,
                    branch TEXT,
                    worktree TEXT,
                    owned_artifact TEXT,
                    host_id TEXT,
                    process_id INTEGER,
                    process_started_at TEXT,
                    current_phase TEXT NOT NULL,
                    status_line TEXT NOT NULL,
                    next_irreversible_step TEXT,
                    blocker TEXT,
                    pr_url TEXT,
                    registered_at TEXT,
                    last_self_reported_at TEXT,
                    effective_state TEXT,
                    effective_state_updated_at TEXT,
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
                    role TEXT,
                    parent_worker_id TEXT,
                    host_id TEXT,
                    process_id INTEGER,
                    process_started_at TEXT,
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

                CREATE TABLE IF NOT EXISTS operator_commands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    general_worker_id TEXT NOT NULL,
                    dispatch_role TEXT NOT NULL DEFAULT 'general',
                    repo_path TEXT NOT NULL,
                    branch_hint TEXT,
                    operator_instruction TEXT NOT NULL,
                    status TEXT NOT NULL,
                    pid INTEGER NOT NULL,
                    prompt_path TEXT NOT NULL,
                    log_path TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS member_registrations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    member_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    parent_member_id TEXT,
                    repo_path TEXT NOT NULL,
                    branch TEXT,
                    worktree TEXT,
                    owned_artifact TEXT,
                    host_id TEXT,
                    process_id INTEGER,
                    process_started_at TEXT,
                    phase TEXT NOT NULL,
                    status_line TEXT NOT NULL,
                    note TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS member_parent_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    subject_member_id TEXT NOT NULL,
                    reporter_member_id TEXT NOT NULL,
                    event_type TEXT,
                    related_member_id TEXT,
                    observed_phase TEXT,
                    observed_status_line TEXT NOT NULL,
                    observed_state TEXT,
                    blocker TEXT,
                    note TEXT,
                    process_id INTEGER,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS member_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    member_id TEXT NOT NULL,
                    sender_member_id TEXT NOT NULL,
                    sender_role TEXT NOT NULL,
                    message_type TEXT NOT NULL,
                    body TEXT NOT NULL,
                    related_member_id TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS member_heartbeats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    member_id TEXT NOT NULL,
                    host_id TEXT,
                    process_id INTEGER,
                    process_started_at TEXT,
                    observed_alive INTEGER NOT NULL,
                    observed_state TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            self._ensure_column(connection, "workers", "role", "TEXT NOT NULL DEFAULT 'worker'")
            self._ensure_column(connection, "workers", "parent_worker_id", "TEXT")
            self._ensure_column(connection, "workers", "host_id", "TEXT")
            self._ensure_column(connection, "workers", "process_id", "INTEGER")
            self._ensure_column(connection, "workers", "process_started_at", "TEXT")
            self._ensure_column(connection, "workers", "registered_at", "TEXT")
            self._ensure_column(connection, "workers", "last_self_reported_at", "TEXT")
            self._ensure_column(connection, "workers", "effective_state", "TEXT")
            self._ensure_column(connection, "workers", "effective_state_updated_at", "TEXT")
            self._ensure_column(connection, "phase_transitions", "role", "TEXT")
            self._ensure_column(connection, "phase_transitions", "parent_worker_id", "TEXT")
            self._ensure_column(connection, "phase_transitions", "host_id", "TEXT")
            self._ensure_column(connection, "phase_transitions", "process_id", "INTEGER")
            self._ensure_column(connection, "phase_transitions", "process_started_at", "TEXT")
            self._ensure_column(connection, "operator_commands", "dispatch_role", "TEXT NOT NULL DEFAULT 'general'")
            self._ensure_column(connection, "member_parent_reports", "event_type", "TEXT")
            self._ensure_column(connection, "member_parent_reports", "related_member_id", "TEXT")

    def healthcheck(self) -> dict[str, object]:
        with _connect(self.db_path) as connection:
            connection.execute("SELECT 1").fetchone()
            worker_count = connection.execute("SELECT COUNT(*) FROM workers").fetchone()[0]
        return {
            "status": "ok",
            "db_path": str(self.db_path),
            "workers": worker_count,
        }

    def record_event(self, event: WorkerEventCreate) -> WorkerDetail:
        with _connect(self.db_path) as connection:
            current = connection.execute(
                "SELECT * FROM workers WHERE worker_id = ?",
                (event.worker_id,),
            ).fetchone()

            self._validate_worker_auth(current, event.worker_token)
            self._validate_transition(current, event)
            self._validate_member_lineage(
                connection,
                member_id=event.worker_id,
                role=event.role,
                parent_member_id=event.parent_worker_id,
            )

            if current is None:
                connection.execute(
                    """
                    INSERT INTO workers (
                        worker_id, worker_token, role, parent_worker_id, repo_path, branch, worktree, owned_artifact,
                        host_id, process_id, process_started_at, current_phase, status_line, next_irreversible_step,
                        blocker, pr_url, registered_at, last_self_reported_at, effective_state,
                        effective_state_updated_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.worker_id,
                        event.worker_token,
                        event.role.value,
                        event.parent_worker_id,
                        event.repo_path,
                        event.branch,
                        event.worktree,
                        event.owned_artifact,
                        event.host_id,
                        event.process_id,
                        event.process_started_at.isoformat() if event.process_started_at else None,
                        event.current_phase.value,
                        event.status_line,
                        event.next_irreversible_step,
                        event.blocker,
                        str(event.pr_url) if event.pr_url else None,
                        event.timestamp.isoformat(),
                        event.timestamp.isoformat(),
                        "active",
                        event.timestamp.isoformat(),
                        event.timestamp.isoformat(),
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE workers
                    SET role = ?, parent_worker_id = ?, repo_path = ?, branch = ?, worktree = ?, owned_artifact = ?,
                        host_id = COALESCE(?, host_id), process_id = COALESCE(?, process_id),
                        process_started_at = COALESCE(?, process_started_at), current_phase = ?, status_line = ?,
                        next_irreversible_step = ?, blocker = ?, pr_url = ?, last_self_reported_at = ?, updated_at = ?
                    WHERE worker_id = ?
                    """,
                    (
                        event.role.value,
                        event.parent_worker_id,
                        event.repo_path,
                        event.branch,
                        event.worktree,
                        event.owned_artifact,
                        event.host_id,
                        event.process_id,
                        event.process_started_at.isoformat() if event.process_started_at else None,
                        event.current_phase.value,
                        event.status_line,
                        event.next_irreversible_step,
                        event.blocker,
                        str(event.pr_url) if event.pr_url else None,
                        event.timestamp.isoformat(),
                        event.timestamp.isoformat(),
                        event.worker_id,
                    ),
                )

            connection.execute(
                """
                INSERT INTO phase_transitions (
                    worker_id, previous_phase, current_phase, status_line, next_irreversible_step,
                    blocker, note, repo_path, branch, worktree, owned_artifact, role, parent_worker_id,
                    host_id, process_id, process_started_at, pr_url, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    event.role.value,
                    event.parent_worker_id,
                    event.host_id,
                    event.process_id,
                    event.process_started_at.isoformat() if event.process_started_at else None,
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

        detail = self.get_worker(event.worker_id)
        _log_event(
            "self_report_recorded",
            worker_id=event.worker_id,
            role=event.role.value,
            phase=event.current_phase.value,
            parent_worker_id=event.parent_worker_id,
            process_id=event.process_id,
            effective_state=detail.effective_state,
        )
        return detail

    def register_member(self, registration: RegistrationCreate) -> WorkerDetail:
        with _connect(self.db_path) as connection:
            current = connection.execute(
                "SELECT * FROM workers WHERE worker_id = ?",
                (registration.member_id,),
            ).fetchone()
            parent = self._resolve_parent_member(connection, registration.parent_member_id)
            self._validate_member_lineage(
                connection,
                member_id=registration.member_id,
                role=registration.role,
                parent_member_id=registration.parent_member_id,
            )
            if (
                parent is not None
                and parent["source"] == "worker"
                and registration.parent_token
                and parent["row"]["worker_token"] != registration.parent_token
            ):
                raise WorkerAuthError("parent token does not match existing parent")

            if current is None:
                connection.execute(
                    """
                    INSERT INTO workers (
                        worker_id, worker_token, role, parent_worker_id, repo_path, branch, worktree, owned_artifact,
                        host_id, process_id, process_started_at, current_phase, status_line, next_irreversible_step,
                        blocker, pr_url, registered_at, last_self_reported_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        registration.member_id,
                        registration.member_token,
                        registration.role.value,
                        registration.parent_member_id,
                        registration.repo_path,
                        registration.branch,
                        registration.worktree,
                        registration.owned_artifact,
                        registration.host_id,
                        registration.process_id,
                        registration.process_started_at.isoformat() if registration.process_started_at else None,
                        registration.phase.value,
                        registration.status_line,
                        None,
                        None,
                        None,
                        registration.timestamp.isoformat(),
                        None,
                        registration.timestamp.isoformat(),
                    ),
                )
            else:
                self._validate_worker_auth(current, registration.member_token)
                connection.execute(
                    """
                    UPDATE workers
                    SET role = ?, parent_worker_id = ?, repo_path = ?, branch = ?, worktree = ?, owned_artifact = ?,
                        host_id = COALESCE(?, host_id), process_id = COALESCE(?, process_id),
                        process_started_at = COALESCE(?, process_started_at), status_line = ?, registered_at = ?
                    WHERE worker_id = ?
                    """,
                    (
                        registration.role.value,
                        registration.parent_member_id,
                        registration.repo_path,
                        registration.branch,
                        registration.worktree,
                        registration.owned_artifact,
                        registration.host_id,
                        registration.process_id,
                        registration.process_started_at.isoformat() if registration.process_started_at else None,
                        registration.status_line,
                        registration.timestamp.isoformat(),
                        registration.member_id,
                    ),
                )

            connection.execute(
                """
                INSERT INTO member_registrations (
                    member_id, role, parent_member_id, repo_path, branch, worktree, owned_artifact,
                    host_id, process_id, process_started_at, phase, status_line, note, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    registration.member_id,
                    registration.role.value,
                    registration.parent_member_id,
                    registration.repo_path,
                    registration.branch,
                    registration.worktree,
                    registration.owned_artifact,
                    registration.host_id,
                    registration.process_id,
                    registration.process_started_at.isoformat() if registration.process_started_at else None,
                    registration.phase.value,
                    registration.status_line,
                    registration.note,
                    registration.timestamp.isoformat(),
                ),
            )

        detail = self.get_worker(registration.member_id)
        _log_event(
            "member_registered",
            member_id=registration.member_id,
            role=registration.role.value,
            parent_member_id=registration.parent_member_id,
            process_id=registration.process_id,
            host_id=registration.host_id,
            phase=registration.phase.value,
        )
        return detail

    def record_member_message(self, message: MemberMessageCreate) -> MemberMessageRecord:
        with _connect(self.db_path) as connection:
            sender = connection.execute(
                "SELECT * FROM workers WHERE worker_id = ?",
                (message.sender_member_id,),
            ).fetchone()
            if sender is None:
                raise KeyError(message.sender_member_id)
            self._validate_worker_auth(sender, message.sender_token)

            subject = connection.execute(
                "SELECT * FROM workers WHERE worker_id = ?",
                (message.member_id,),
            ).fetchone()
            if subject is None:
                raise KeyError(message.member_id)

            if message.sender_member_id != message.member_id and subject["parent_worker_id"] != message.sender_member_id:
                raise WorkerAuthError("sender may only message itself or a direct child")

            cursor = connection.execute(
                """
                INSERT INTO member_messages (
                    member_id, sender_member_id, sender_role, message_type, body, related_member_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message.member_id,
                    message.sender_member_id,
                    sender["role"],
                    message.message_type,
                    message.body,
                    message.related_member_id,
                    message.timestamp.isoformat(),
                ),
            )

        created = MemberMessageRecord(
            id=cursor.lastrowid,
            member_id=message.member_id,
            sender_member_id=message.sender_member_id,
            sender_role=MemberRole(sender["role"]),
            message_type=message.message_type,
            body=message.body,
            related_member_id=message.related_member_id,
            created_at=message.timestamp,
        )
        _log_event(
            "member_message_recorded",
            member_id=message.member_id,
            sender_member_id=message.sender_member_id,
            sender_role=sender["role"],
            message_type=message.message_type,
            related_member_id=message.related_member_id,
        )
        return created

    def list_member_messages(self, member_id: str, limit: int = 20) -> list[MemberMessageRecord]:
        with _connect(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT * FROM member_messages
                WHERE member_id = ?
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT ?
                """,
                (member_id, limit),
            ).fetchall()
        return [self._row_to_message(row) for row in rows]

    def record_parent_report(self, report: ParentReportCreate) -> ParentReportRecord:
        with _connect(self.db_path) as connection:
            reporter = connection.execute(
                "SELECT * FROM workers WHERE worker_id = ?",
                (report.reporter_member_id,),
            ).fetchone()
            if reporter is None:
                raise KeyError(report.reporter_member_id)
            self._validate_worker_auth(reporter, report.reporter_token)
            subject = connection.execute(
                "SELECT * FROM workers WHERE worker_id = ?",
                (report.subject_member_id,),
            ).fetchone()
            if subject is None:
                raise KeyError(report.subject_member_id)

            cursor = connection.execute(
                """
                INSERT INTO member_parent_reports (
                    subject_member_id, reporter_member_id, event_type, related_member_id,
                    observed_phase, observed_status_line, observed_state, blocker, note, process_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report.subject_member_id,
                    report.reporter_member_id,
                    report.event_type,
                    report.related_member_id,
                    report.observed_phase.value if report.observed_phase else None,
                    report.observed_status_line,
                    report.observed_state,
                    report.blocker,
                    report.note,
                    report.process_id,
                    report.timestamp.isoformat(),
                ),
            )

        created = ParentReportRecord(
            id=cursor.lastrowid,
            subject_member_id=report.subject_member_id,
            reporter_member_id=report.reporter_member_id,
            event_type=report.event_type,
            related_member_id=report.related_member_id,
            observed_phase=report.observed_phase,
            observed_status_line=report.observed_status_line,
            observed_state=report.observed_state,
            blocker=report.blocker,
            note=report.note,
            process_id=report.process_id,
            created_at=report.timestamp,
        )
        _log_event(
            "parent_report_recorded",
            subject_member_id=report.subject_member_id,
            reporter_member_id=report.reporter_member_id,
            event_type=report.event_type,
            related_member_id=report.related_member_id,
            observed_phase=report.observed_phase.value if report.observed_phase else None,
            observed_state=report.observed_state,
            process_id=report.process_id,
        )
        return created

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

    def record_command(
        self,
        command: OperatorCommandCreate,
        launch: OperatorCommandLaunch,
    ) -> OperatorCommandRecord:
        with _connect(self.db_path) as connection:
            cursor = connection.execute(
                """
                INSERT INTO operator_commands (
                    general_worker_id, dispatch_role, repo_path, branch_hint, operator_instruction,
                    status, pid, prompt_path, log_path, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    command.general_worker_id,
                    command.dispatch_role.value,
                    command.repo_path,
                    command.branch_hint,
                    command.operator_instruction,
                    launch.status.value,
                    launch.pid,
                    launch.prompt_path,
                    launch.log_path,
                    command.created_at.isoformat(),
                ),
            )

        return OperatorCommandRecord(
            id=cursor.lastrowid,
            general_worker_id=command.general_worker_id,
            dispatch_role=command.dispatch_role,
            repo_path=command.repo_path,
            branch_hint=command.branch_hint,
            operator_instruction=command.operator_instruction,
            status=launch.status,
            pid=launch.pid,
            prompt_path=launch.prompt_path,
            log_path=launch.log_path,
            created_at=command.created_at,
        )

    def list_commands(self, limit: int = 12) -> list[OperatorCommandRecord]:
        with _connect(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT * FROM operator_commands
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [self._row_to_command(row) for row in rows]

    def get_worker(self, worker_id: str) -> WorkerDetail:
        self.refresh_heartbeats()
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
            registrations = connection.execute(
                """
                SELECT * FROM member_registrations
                WHERE member_id = ?
                ORDER BY datetime(created_at) DESC, id DESC
                """,
                (worker_id,),
            ).fetchall()
            parent_reports = connection.execute(
                """
                SELECT * FROM member_parent_reports
                WHERE subject_member_id = ?
                ORDER BY datetime(created_at) DESC, id DESC
                """,
                (worker_id,),
            ).fetchall()
            heartbeats = connection.execute(
                """
                SELECT * FROM member_heartbeats
                WHERE member_id = ?
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT 12
                """,
                (worker_id,),
            ).fetchall()
            messages = connection.execute(
                """
                SELECT * FROM member_messages
                WHERE member_id = ?
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT 20
                """,
                (worker_id,),
            ).fetchall()

        summary = self._row_to_summary(
            current,
            self._row_to_note(notes[0]) if notes else None,
            self._row_to_parent_report(parent_reports[0]) if parent_reports else None,
            self._row_to_heartbeat(heartbeats[0]) if heartbeats else None,
            self._row_to_message(messages[0]) if messages else None,
        )
        return WorkerDetail(
            **summary.model_dump(),
            registrations=[self._row_to_registration(row) for row in registrations],
            parent_reports=[self._row_to_parent_report(row) for row in parent_reports],
            heartbeats=[self._row_to_heartbeat(row) for row in heartbeats],
            messages=[self._row_to_message(row) for row in messages],
            transitions=[self._row_to_transition(row) for row in transitions],
            notes=[self._row_to_note(row) for row in notes],
        )

    def snapshot(self) -> DashboardSnapshot:
        self.refresh_heartbeats()
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
            latest_parent_report_rows = connection.execute(
                """
                SELECT pr.*
                FROM member_parent_reports pr
                JOIN (
                    SELECT subject_member_id, MAX(id) AS latest_id
                    FROM member_parent_reports
                    GROUP BY subject_member_id
                ) latest ON latest.latest_id = pr.id
                """
            ).fetchall()
            latest_heartbeat_rows = connection.execute(
                """
                SELECT hb.*
                FROM member_heartbeats hb
                JOIN (
                    SELECT member_id, MAX(id) AS latest_id
                    FROM member_heartbeats
                    GROUP BY member_id
                ) latest ON latest.latest_id = hb.id
                """
            ).fetchall()
            latest_message_rows = connection.execute(
                """
                SELECT mm.*
                FROM member_messages mm
                JOIN (
                    SELECT member_id, MAX(id) AS latest_id
                    FROM member_messages
                    GROUP BY member_id
                ) latest ON latest.latest_id = mm.id
                """
            ).fetchall()

        latest_notes = {
            row["worker_id"]: self._row_to_note(row)
            for row in latest_note_rows
        }
        latest_parent_reports = {
            row["subject_member_id"]: self._row_to_parent_report(row)
            for row in latest_parent_report_rows
        }
        latest_heartbeats = {
            row["member_id"]: self._row_to_heartbeat(row)
            for row in latest_heartbeat_rows
        }
        latest_messages = {
            row["member_id"]: self._row_to_message(row)
            for row in latest_message_rows
        }
        summaries = [
            self._row_to_summary(
                row,
                latest_notes.get(row["worker_id"]),
                latest_parent_reports.get(row["worker_id"]),
                latest_heartbeats.get(row["worker_id"]),
                latest_messages.get(row["worker_id"]),
            )
            for row in workers
        ]

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

    def refresh_heartbeats(self) -> None:
        now = datetime.now(timezone.utc)
        with _connect(self.db_path) as connection:
            workers = connection.execute(
                """
                SELECT worker_id, host_id, process_id, process_started_at, current_phase,
                       effective_state, blocker, updated_at
                FROM workers
                """
            ).fetchall()
            latest_parent_reports = {
                row["subject_member_id"]: row
                for row in connection.execute(
                    """
                    SELECT pr.*
                    FROM member_parent_reports pr
                    JOIN (
                        SELECT subject_member_id, MAX(id) AS latest_id
                        FROM member_parent_reports
                        GROUP BY subject_member_id
                    ) latest ON latest.latest_id = pr.id
                    """
                ).fetchall()
            }
            for row in workers:
                has_process_identity = bool(row["process_id"])
                observed_alive = self._pid_alive(row["process_id"]) if has_process_identity else False
                observed_state = "alive" if observed_alive else ("missing" if has_process_identity else "untracked")
                connection.execute(
                    """
                    INSERT INTO member_heartbeats (
                        member_id, host_id, process_id, process_started_at, observed_alive, observed_state, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["worker_id"],
                        row["host_id"],
                        row["process_id"],
                        row["process_started_at"],
                        1 if observed_alive else 0,
                        observed_state,
                        now.isoformat(),
                    ),
                )
                _log_event(
                    "heartbeat_observed",
                    member_id=row["worker_id"],
                    host_id=row["host_id"],
                    process_id=row["process_id"],
                    observed_alive=observed_alive,
                    observed_state=observed_state,
                )
                next_effective_state = self._effective_state(
                    row,
                    self._row_to_parent_report(latest_parent_reports[row["worker_id"]])
                    if row["worker_id"] in latest_parent_reports
                    else None,
                    HeartbeatRecord(
                        id=0,
                        member_id=row["worker_id"],
                        host_id=row["host_id"],
                        process_id=row["process_id"],
                        process_started_at=(
                            self._parse_timestamp(row["process_started_at"])
                            if row["process_started_at"]
                            else None
                        ),
                        observed_alive=observed_alive,
                        observed_state=observed_state,
                        created_at=now,
                    ),
                )
                if row["effective_state"] != next_effective_state:
                    connection.execute(
                        """
                        UPDATE workers
                        SET effective_state = ?, effective_state_updated_at = ?
                        WHERE worker_id = ?
                        """,
                        (next_effective_state, now.isoformat(), row["worker_id"]),
                    )
                    _log_event(
                        "effective_state_changed",
                        member_id=row["worker_id"],
                        previous_state=row["effective_state"],
                        next_state=next_effective_state,
                    )

    def _validate_worker_auth(self, current: sqlite3.Row | None, worker_token: str) -> None:
        if current is not None and current["worker_token"] != worker_token:
            raise WorkerAuthError("worker token does not match existing worker")

    def _resolve_parent_member(
        self,
        connection: sqlite3.Connection,
        parent_member_id: str | None,
    ) -> dict[str, object] | None:
        if not parent_member_id:
            return None
        parent_worker = connection.execute(
            "SELECT * FROM workers WHERE worker_id = ?",
            (parent_member_id,),
        ).fetchone()
        if parent_worker is not None:
            return {"source": "worker", "role": MemberRole(parent_worker["role"]), "row": parent_worker}
        parent_command = connection.execute(
            """
            SELECT * FROM operator_commands
            WHERE general_worker_id = ?
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT 1
            """,
            (parent_member_id,),
        ).fetchone()
        if parent_command is not None:
            return {"source": "command", "role": MemberRole.GENERAL, "row": parent_command}
        raise KeyError(parent_member_id)

    def _validate_member_lineage(
        self,
        connection: sqlite3.Connection,
        *,
        member_id: str,
        role: MemberRole,
        parent_member_id: str | None,
    ) -> None:
        if role == MemberRole.GENERAL:
            if parent_member_id is not None:
                raise ValueError("general members may not declare a parent")
            return
        if role == MemberRole.CAPTAIN and parent_member_id is None:
            root_command = connection.execute(
                """
                SELECT * FROM operator_commands
                WHERE general_worker_id = ? AND dispatch_role = ?
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT 1
                """,
                (member_id, DispatchRole.CAPTAIN.value),
            ).fetchone()
            if root_command is not None:
                return
        parent = self._resolve_parent_member(connection, parent_member_id)
        if parent is None:
            if role == MemberRole.WORKER:
                return
            raise ValueError(f"{role.value} members must declare a parent")
        parent_role = parent["role"]
        if role == MemberRole.CAPTAIN and parent_role != MemberRole.GENERAL:
            raise ValueError("captain parent must be a general")
        if role == MemberRole.WORKER and parent_role != MemberRole.CAPTAIN:
            raise ValueError("worker parent must be a captain")

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

    def _row_to_summary(
        self,
        row: sqlite3.Row,
        last_note: WorkerNoteRecord | None,
        last_parent_report: ParentReportRecord | None,
        last_heartbeat: HeartbeatRecord | None,
        last_message: MemberMessageRecord | None,
    ) -> WorkerSummary:
        return WorkerSummary(
            worker_id=row["worker_id"],
            role=MemberRole(row["role"]) if row["role"] else MemberRole.WORKER,
            parent_worker_id=row["parent_worker_id"],
            phase=WorkerPhase(row["current_phase"]),
            status_line=row["status_line"],
            repo_path=row["repo_path"],
            branch=row["branch"],
            worktree=row["worktree"],
            owned_artifact=row["owned_artifact"],
            host_id=row["host_id"],
            process_id=row["process_id"],
            process_started_at=self._parse_timestamp(row["process_started_at"]) if row["process_started_at"] else None,
            next_irreversible_step=row["next_irreversible_step"],
            blocker=row["blocker"],
            pr_url=row["pr_url"],
            updated_at=self._parse_timestamp(row["updated_at"]),
            registered_at=self._parse_timestamp(row["registered_at"]) if row["registered_at"] else None,
            last_self_reported_at=(
                self._parse_timestamp(row["last_self_reported_at"])
                if row["last_self_reported_at"]
                else None
            ),
            last_parent_report=last_parent_report,
            last_heartbeat=last_heartbeat,
            effective_state=(
                row["effective_state"]
                or self._effective_state(row, last_parent_report, last_heartbeat)
            ),
            last_note=last_note,
            last_message=last_message,
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
            role=MemberRole(row["role"]) if row["role"] else MemberRole.WORKER,
            parent_worker_id=row["parent_worker_id"],
            host_id=row["host_id"],
            process_id=row["process_id"],
            process_started_at=self._parse_timestamp(row["process_started_at"]) if row["process_started_at"] else None,
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

    def _row_to_command(self, row: sqlite3.Row) -> OperatorCommandRecord:
        return OperatorCommandRecord(
            id=row["id"],
            general_worker_id=row["general_worker_id"],
            dispatch_role=DispatchRole(row["dispatch_role"]) if row["dispatch_role"] else DispatchRole.GENERAL,
            repo_path=row["repo_path"],
            branch_hint=row["branch_hint"],
            operator_instruction=row["operator_instruction"],
            status=row["status"],
            pid=row["pid"],
            prompt_path=row["prompt_path"],
            log_path=row["log_path"],
            created_at=self._parse_timestamp(row["created_at"]),
        )

    def _row_to_registration(self, row: sqlite3.Row) -> RegistrationRecord:
        return RegistrationRecord(
            id=row["id"],
            member_id=row["member_id"],
            role=MemberRole(row["role"]),
            parent_member_id=row["parent_member_id"],
            repo_path=row["repo_path"],
            branch=row["branch"],
            worktree=row["worktree"],
            owned_artifact=row["owned_artifact"],
            host_id=row["host_id"],
            process_id=row["process_id"],
            process_started_at=self._parse_timestamp(row["process_started_at"]) if row["process_started_at"] else None,
            phase=WorkerPhase(row["phase"]),
            status_line=row["status_line"],
            note=row["note"],
            created_at=self._parse_timestamp(row["created_at"]),
        )

    def _row_to_parent_report(self, row: sqlite3.Row) -> ParentReportRecord:
        return ParentReportRecord(
            id=row["id"],
            subject_member_id=row["subject_member_id"],
            reporter_member_id=row["reporter_member_id"],
            event_type=row["event_type"],
            related_member_id=row["related_member_id"],
            observed_phase=WorkerPhase(row["observed_phase"]) if row["observed_phase"] else None,
            observed_status_line=row["observed_status_line"],
            observed_state=row["observed_state"],
            blocker=row["blocker"],
            note=row["note"],
            process_id=row["process_id"],
            created_at=self._parse_timestamp(row["created_at"]),
        )

    def _row_to_heartbeat(self, row: sqlite3.Row) -> HeartbeatRecord:
        return HeartbeatRecord(
            id=row["id"],
            member_id=row["member_id"],
            host_id=row["host_id"],
            process_id=row["process_id"],
            process_started_at=self._parse_timestamp(row["process_started_at"]) if row["process_started_at"] else None,
            observed_alive=bool(row["observed_alive"]),
            observed_state=row["observed_state"],
            created_at=self._parse_timestamp(row["created_at"]),
        )

    def _row_to_message(self, row: sqlite3.Row) -> MemberMessageRecord:
        return MemberMessageRecord(
            id=row["id"],
            member_id=row["member_id"],
            sender_member_id=row["sender_member_id"],
            sender_role=MemberRole(row["sender_role"]),
            message_type=row["message_type"],
            body=row["body"],
            related_member_id=row["related_member_id"],
            created_at=self._parse_timestamp(row["created_at"]),
        )

    def _effective_state(
        self,
        row: sqlite3.Row,
        last_parent_report: ParentReportRecord | None,
        last_heartbeat: HeartbeatRecord | None,
    ) -> str:
        if row["current_phase"] == WorkerPhase.BLOCKED.value:
            return "blocked"
        if last_parent_report and last_parent_report.observed_state:
            if last_parent_report.observed_state in {
                "missing",
                "silent",
                "disputed",
                "blocked",
                "terminated",
                "replaced",
                "failed",
                "complete",
            }:
                return last_parent_report.observed_state
        if row["current_phase"] == WorkerPhase.TERMINAL.value:
            return "done"
        if last_heartbeat and last_heartbeat.observed_state == "missing":
            return "lost"
        return "active"

    def _pid_alive(self, process_id: int | None) -> bool:
        if process_id is None:
            return False
        try:
            os.kill(process_id, 0)
        except OSError:
            return False
        return True

    def _ensure_column(
        self,
        connection: sqlite3.Connection,
        table: str,
        column: str,
        ddl: str,
    ) -> None:
        existing = {
            row["name"]
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in existing:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    def _parse_timestamp(self, value: str) -> datetime:
        return datetime.fromisoformat(value)
