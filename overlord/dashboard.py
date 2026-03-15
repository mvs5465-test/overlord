from __future__ import annotations

from datetime import datetime, timezone

from overlord.models import DashboardSnapshot, PHASE_ORDER, WorkerDetail, WorkerPhase


AGING_AFTER_SECONDS = 8 * 60
STALE_AFTER_SECONDS = 20 * 60


def format_relative_time(value: datetime) -> str:
    now = datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    delta_seconds = int((now - value.astimezone(timezone.utc)).total_seconds())
    if delta_seconds < 60:
        return "just now"
    if delta_seconds < 3600:
        minutes = delta_seconds // 60
        return f"{minutes}m ago"
    if delta_seconds < 86400:
        hours = delta_seconds // 3600
        return f"{hours}h ago"
    days = delta_seconds // 86400
    return f"{days}d ago"


def format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def worker_freshness(value: datetime) -> dict[str, str | int]:
    now = datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    age_seconds = max(0, int((now - value.astimezone(timezone.utc)).total_seconds()))
    age_minutes = age_seconds // 60
    if age_seconds >= STALE_AFTER_SECONDS:
        state = "stale"
    elif age_seconds >= AGING_AFTER_SECONDS:
        state = "aging"
    else:
        state = "fresh"
    return {
        "state": state,
        "label": f"{format_relative_time(value)} update",
        "age_minutes": age_minutes,
    }


def pick_focus_worker(snapshot: DashboardSnapshot, requested_worker_id: str | None) -> str | None:
    worker_ids = {worker.worker_id for worker in snapshot.workers}
    if requested_worker_id in worker_ids:
        return requested_worker_id
    if not snapshot.workers:
        return None

    priority = {
        WorkerPhase.BLOCKED: 0,
        WorkerPhase.HANDOFF_READY: 1,
        WorkerPhase.VALIDATING: 2,
        WorkerPhase.IMPLEMENTING: 3,
        WorkerPhase.PLANNED: 4,
        WorkerPhase.SCOUTING: 5,
        WorkerPhase.ASSIGNED: 6,
        WorkerPhase.TERMINAL: 7,
    }
    ranked = sorted(
        snapshot.workers,
        key=lambda worker: (
            priority[worker.phase],
            worker.updated_at.timestamp() * -1,
            worker.worker_id,
        ),
    )
    return ranked[0].worker_id


def grouped_phase_notes(worker: WorkerDetail) -> list[dict[str, object]]:
    grouped: list[dict[str, object]] = []
    for phase in PHASE_ORDER:
        notes = [note for note in worker.notes if note.phase == phase]
        if notes:
            grouped.append({"phase": phase, "notes": notes})
    return grouped
