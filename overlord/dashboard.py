from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from overlord.models import (
    ConflictRecord,
    DashboardSnapshot,
    DispatchRole,
    MemberRole,
    OperatorCommandRecord,
    PHASE_ORDER,
    WorkerDetail,
    WorkerPhase,
)


QUIET_AFTER_SECONDS = 5 * 60
STALE_AFTER_SECONDS = 20 * 60

MISSION_VIEWS = ["missions", "board", "fanout", "dispatches", "conflicts", "workers"]
SAVED_VIEW_DEFS = [
    ("all-active", "All Active"),
    ("needs-attention", "Needs Attention"),
    ("merge-work", "Merge Work"),
    ("stale-only", "Stale Only"),
    ("solo-workers", "Solo Workers"),
]
MERGE_ORDER = [
    "no_branch",
    "branch_active",
    "pr_open",
    "changes_requested",
    "approved",
    "merged_to_main",
    "synced_locally",
]
MERGE_LABELS = {
    "no_branch": "no branch",
    "branch_active": "branch active",
    "pr_open": "pr open",
    "changes_requested": "changes requested",
    "approved": "approved",
    "merged_to_main": "merged to main",
    "synced_locally": "synced locally",
}
PHASE_PRIORITY = {
    WorkerPhase.BLOCKED: 0,
    WorkerPhase.HANDOFF_READY: 1,
    WorkerPhase.VALIDATING: 2,
    WorkerPhase.IMPLEMENTING: 3,
    WorkerPhase.PLANNED: 4,
    WorkerPhase.SCOUTING: 5,
    WorkerPhase.ASSIGNED: 6,
    WorkerPhase.TERMINAL: 7,
}
SUPERVISION_PHASE_LABELS = {
    WorkerPhase.ASSIGNED: "assigned",
    WorkerPhase.SCOUTING: "scouting",
    WorkerPhase.PLANNED: "planned",
    WorkerPhase.IMPLEMENTING: "implementing",
    WorkerPhase.VALIDATING: "validating",
    WorkerPhase.BLOCKED: "blocked",
    WorkerPhase.HANDOFF_READY: "ready for review",
    WorkerPhase.TERMINAL: "terminal",
}


def format_relative_time(value: datetime) -> str:
    delta_seconds = _age_seconds(value)
    if delta_seconds < 60:
        return "just now"
    if delta_seconds < 3600:
        return f"{delta_seconds // 60}m ago"
    if delta_seconds < 86400:
        return f"{delta_seconds // 3600}h ago"
    return f"{delta_seconds // 86400}d ago"


def format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def worker_freshness(value: datetime) -> dict[str, str | int]:
    age_seconds = _age_seconds(value)
    age_minutes = age_seconds // 60
    if age_seconds >= STALE_AFTER_SECONDS:
        state = "stale"
        label = f"stale {max(1, age_minutes)}m"
    elif age_seconds >= QUIET_AFTER_SECONDS:
        state = "quiet"
        label = f"quiet {max(1, age_minutes)}m"
    else:
        state = "active"
        label = "active now" if age_seconds < 60 else f"active {max(1, age_minutes)}m"
    return {"state": state, "label": label, "age_minutes": age_minutes}


def pick_focus_worker(snapshot: DashboardSnapshot, requested_worker_id: str | None) -> str | None:
    worker_ids = {worker.worker_id for worker in snapshot.workers}
    if requested_worker_id in worker_ids:
        return requested_worker_id
    if not snapshot.workers:
        return None
    ranked = sorted(
        snapshot.workers,
        key=lambda worker: (
            PHASE_PRIORITY[worker.phase],
            -worker.updated_at.timestamp(),
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


def build_supervision_view(
    snapshot: DashboardSnapshot,
    worker_details: dict[str, WorkerDetail],
    recent_commands: list[OperatorCommandRecord],
    *,
    requested_worker_id: str | None,
    requested_mission_id: str | None,
    search_query: str,
    current_view: str,
    saved_view: str,
) -> dict[str, Any]:
    normalized_search = search_query.strip().lower()
    worker_states = {
        worker.worker_id: worker_freshness(worker.updated_at)
        for worker in snapshot.workers
    }
    conflicts_by_worker = _index_conflicts(snapshot.conflicts)
    mission_rows = _build_mission_rows(
        snapshot,
        worker_details,
        recent_commands,
        worker_states,
        conflicts_by_worker,
    )
    mission_rows = _apply_search(mission_rows, normalized_search)
    mission_rows = _apply_saved_view(mission_rows, saved_view)
    attention_items = _build_attention_queue(mission_rows)
    selected_worker_id = _pick_selected_worker_id(mission_rows, requested_worker_id)
    selected_mission_id = _pick_selected_mission_id(
        mission_rows,
        requested_mission_id=requested_mission_id,
        requested_worker_id=selected_worker_id,
    )

    for mission in mission_rows:
        mission["selected"] = mission["id"] == selected_mission_id
        for worker in mission["workers"]:
            worker["selected"] = worker["worker_id"] == selected_worker_id

    selected_mission = next(
        (mission for mission in mission_rows if mission["id"] == selected_mission_id),
        None,
    )
    selected_worker = (
        worker_details[selected_worker_id] if selected_worker_id and selected_worker_id in worker_details else None
    )

    return {
        "current_view": current_view if current_view in MISSION_VIEWS else "missions",
        "saved_view": saved_view if saved_view in {key for key, _ in SAVED_VIEW_DEFS} else "all-active",
        "search_query": search_query,
        "search_active": bool(normalized_search),
        "worker_states": worker_states,
        "selected_worker_id": selected_worker_id,
        "selected_worker": selected_worker,
        "selected_worker_notes": grouped_phase_notes(selected_worker) if selected_worker else [],
        "selected_mission_id": selected_mission_id,
        "selected_mission": selected_mission,
        "mission_rows": mission_rows,
        "attention_items": attention_items,
        "summary": _build_summary(mission_rows, attention_items),
        "mission_board": _build_mission_board(mission_rows),
        "fanout_clusters": _build_fanout_clusters(mission_rows),
        "dispatch_rows": _build_dispatch_rows(recent_commands, mission_rows),
        "conflict_groups": _build_conflict_groups(mission_rows),
        "worker_rows": _build_worker_rows(mission_rows),
        "saved_views": [
            {"key": key, "label": label, "selected": key == saved_view}
            for key, label in SAVED_VIEW_DEFS
        ],
        "view_tabs": [
            {"key": key, "label": _titleize(key), "selected": key == current_view}
            for key in MISSION_VIEWS
        ],
    }


def build_graph_view(
    supervision: dict[str, Any],
    worker_details: dict[str, WorkerDetail],
    recent_commands: list[OperatorCommandRecord],
    *,
    selected_general_id: str | None = None,
) -> dict[str, Any]:
    latest_command_by_root: dict[str, OperatorCommandRecord] = {}
    for command in sorted(
        recent_commands,
        key=lambda item: (item.created_at, item.id),
        reverse=True,
    ):
        latest_command_by_root.setdefault(command.general_worker_id, command)

    latest_activity_at = max(
        [
            *(detail.updated_at for detail in worker_details.values()),
            *(command.created_at for command in latest_command_by_root.values()),
        ],
        default=datetime.now(timezone.utc),
    )
    nodes: list[dict[str, Any]] = [
        {
            "id": "overlord",
            "role": "overlord",
            "display_type": "overlord",
            "status_label": _node_status_label("active"),
            "age_label": _short_age_label(latest_activity_at),
            "state": "active",
            "detail": {
                "title": "OVERLORD",
                "subtitle": "control plane",
                "state": "active",
                "mission": "Central coordinator for generals, captains, and workers.",
                "messages": [],
                "extended": [
                    {"label": "Missions", "value": str(supervision["summary"]["missions"])},
                    {"label": "Workers", "value": str(supervision["summary"]["workers"])},
                    {"label": "Attention", "value": str(supervision["summary"]["attention"])},
                ],
                "actions": [],
            },
        }
    ]
    edges: list[dict[str, str]] = []
    seen_ids = {"overlord"}
    edge_pairs: set[tuple[str, str]] = set()
    orphan_worker_clusters: dict[str, str] = {}

    def add_edge(source: str, target: str) -> None:
        pair = (source, target)
        if pair not in edge_pairs:
            edges.append({"source": source, "target": target})
            edge_pairs.add(pair)

    def add_node(node: dict[str, Any]) -> None:
        if node["id"] not in seen_ids:
            nodes.append(node)
            seen_ids.add(node["id"])

    for root_worker_id, command in sorted(
        latest_command_by_root.items(),
        key=lambda item: (item[1].created_at, item[0]),
    ):
        add_node(_build_root_graph_node(root_worker_id, worker_details.get(root_worker_id), command))
        add_edge("overlord", _command_root_node_id(root_worker_id, command))

    for detail in sorted(
        worker_details.values(),
        key=lambda item: (_graph_role_order(item.role), item.updated_at.timestamp(), item.worker_id),
    ):
        if detail.role == MemberRole.GENERAL:
            add_node(_build_root_graph_node(detail.worker_id, detail, latest_command_by_root.get(detail.worker_id)))
            add_edge("overlord", _member_node_id(detail.worker_id, detail.role))
            continue

        add_node(_build_worker_graph_node(detail))
        parent_node_id = _resolve_graph_parent_id(
            detail.parent_worker_id,
            worker_details,
            latest_command_by_root,
        )
        if parent_node_id is None and detail.role == MemberRole.CAPTAIN and detail.worker_id in latest_command_by_root:
            parent_node_id = "overlord"
        if parent_node_id is None and detail.role == MemberRole.WORKER:
            parent_node_id = _ensure_orphan_worker_cluster(
                detail,
                nodes,
                seen_ids,
                orphan_worker_clusters,
            )
            add_edge("overlord", parent_node_id)
        if parent_node_id is None:
            parent_node_id = "overlord"
        add_edge(parent_node_id, _member_node_id(detail.worker_id, detail.role))

    return {
        "nodes": nodes,
        "edges": edges,
        "selectedNodeId": (
            _selected_root_node_id(selected_general_id, latest_command_by_root) if selected_general_id else None
        ),
    }


def _build_mission_rows(
    snapshot: DashboardSnapshot,
    worker_details: dict[str, WorkerDetail],
    recent_commands: list[OperatorCommandRecord],
    worker_states: dict[str, dict[str, str | int]],
    conflicts_by_worker: dict[str, list[ConflictRecord]],
) -> list[dict[str, Any]]:
    commands_by_root: dict[str, list[OperatorCommandRecord]] = defaultdict(list)
    for command in recent_commands:
        commands_by_root[command.general_worker_id].append(command)

    remaining_workers = {worker.worker_id: worker for worker in snapshot.workers}
    missions: list[dict[str, Any]] = []

    for root_worker_id, commands in sorted(
        commands_by_root.items(),
        key=lambda item: max(command.created_at for command in item[1]),
        reverse=True,
    ):
        latest_command = max(commands, key=lambda command: (command.created_at, command.id))
        matched_workers = [
            worker
            for worker in list(remaining_workers.values())
            if _lineage_root_id(worker, worker_details) == root_worker_id
            or worker.worker_id == root_worker_id
        ]
        for worker in list(remaining_workers.values()):
            if worker in matched_workers:
                continue
        for worker in matched_workers:
            remaining_workers.pop(worker.worker_id, None)
        missions.append(
            _build_mission(
                workers=matched_workers,
                worker_details=worker_details,
                commands=sorted(commands, key=lambda command: (command.created_at, command.id), reverse=True),
                worker_states=worker_states,
                conflicts_by_worker=conflicts_by_worker,
                seed_repo=latest_command.repo_path,
                owner_hint=root_worker_id,
                branch_hint=latest_command.branch_hint,
                goal_hint=latest_command.operator_instruction,
            )
        )

    lineage_groups: dict[str, list[Any]] = defaultdict(list)
    for worker in list(remaining_workers.values()):
        lineage_root_id = _lineage_root_id(worker, worker_details)
        if lineage_root_id:
            lineage_groups[lineage_root_id].append(worker)

    for root_worker_id, workers in sorted(
        lineage_groups.items(),
        key=lambda item: max(worker.updated_at for worker in item[1]),
        reverse=True,
    ):
        for worker in workers:
            remaining_workers.pop(worker.worker_id, None)
        root_detail = worker_details.get(root_worker_id)
        seed_repo = root_detail.repo_path if root_detail else workers[0].repo_path
        branch_hint = root_detail.branch if root_detail else workers[0].branch
        missions.append(
            _build_mission(
                workers=workers,
                worker_details=worker_details,
                commands=[],
                worker_states=worker_states,
                conflicts_by_worker=conflicts_by_worker,
                seed_repo=seed_repo,
                owner_hint=root_worker_id,
                branch_hint=branch_hint,
                goal_hint=None,
            )
        )

    repo_branch_groups: dict[tuple[str, str], list[Any]] = defaultdict(list)
    solo_workers: list[Any] = []
    for worker in remaining_workers.values():
        if worker.branch:
            repo_branch_groups[(worker.repo_path, worker.branch)].append(worker)
        else:
            solo_workers.append(worker)

    for (repo_path, branch), workers in sorted(
        repo_branch_groups.items(),
        key=lambda item: max(worker.updated_at for worker in item[1]),
        reverse=True,
    ):
        missions.append(
            _build_mission(
                workers=workers,
                worker_details=worker_details,
                commands=[],
                worker_states=worker_states,
                conflicts_by_worker=conflicts_by_worker,
                seed_repo=repo_path,
                owner_hint=None,
                branch_hint=branch,
                goal_hint=None,
            )
        )

    for worker in sorted(solo_workers, key=lambda item: item.updated_at, reverse=True):
        missions.append(
            _build_mission(
                workers=[worker],
                worker_details=worker_details,
                commands=[],
                worker_states=worker_states,
                conflicts_by_worker=conflicts_by_worker,
                seed_repo=worker.repo_path,
                owner_hint=worker.worker_id,
                branch_hint=worker.branch,
                goal_hint=None,
            )
        )

    return sorted(
        missions,
        key=lambda mission: (
            mission["attention_rank"],
            -mission["latest_event_at"].timestamp(),
            mission["repo_name"],
            mission["id"],
        ),
    )


def _build_mission(
    *,
    workers: list[Any],
    worker_details: dict[str, WorkerDetail],
    commands: list[OperatorCommandRecord],
    worker_states: dict[str, dict[str, str | int]],
    conflicts_by_worker: dict[str, list[ConflictRecord]],
    seed_repo: str,
    owner_hint: str | None,
    branch_hint: str | None,
    goal_hint: str | None,
) -> dict[str, Any]:
    worker_dicts: list[dict[str, Any]] = []
    event_candidates: list[tuple[datetime, str, str, str | None]] = []
    worker_ids = []
    conflict_index: dict[tuple[str, str], dict[str, Any]] = {}
    merge_counts = {state: 0 for state in MERGE_ORDER}

    for worker in sorted(
        workers,
        key=lambda item: (PHASE_PRIORITY[item.phase], -item.updated_at.timestamp(), item.worker_id),
    ):
        detail = worker_details[worker.worker_id]
        freshness = worker_states[worker.worker_id]
        merge_state = _infer_merge_state(detail)
        merge_counts[merge_state] += 1
        worker_ids.append(worker.worker_id)
        worker_dicts.append(
            {
                "worker_id": worker.worker_id,
                "phase": worker.phase,
                "phase_label": SUPERVISION_PHASE_LABELS[worker.phase],
                "status_line": worker.status_line,
                "repo_path": worker.repo_path,
                "branch": worker.branch,
                "worktree": worker.worktree,
                "owned_artifact": worker.owned_artifact,
                "next_irreversible_step": worker.next_irreversible_step,
                "blocker": worker.blocker,
                "pr_url": worker.pr_url,
                "updated_at": worker.updated_at,
                "updated_label": freshness["label"],
                "freshness_state": freshness["state"],
                "merge_state": merge_state,
                "merge_label": MERGE_LABELS[merge_state],
                "timeline_count": len(detail.transitions),
                "selected": False,
            }
        )
        event_candidates.append(
            (
                worker.updated_at,
                "worker",
                f"{worker.worker_id} {worker.phase.value}",
                worker.worker_id,
            )
        )
        for conflict in conflicts_by_worker.get(worker.worker_id, []):
            entry = conflict_index.setdefault(
                (conflict.field, conflict.value),
                {
                    "field": conflict.field,
                    "value": conflict.value,
                    "worker_ids": set(),
                },
            )
            entry["worker_ids"].update(conflict.worker_ids)

    for command in commands:
        event_candidates.append(
            (
                command.created_at,
                "dispatch",
                f"dispatched {command.general_worker_id}",
                None,
            )
        )

    latest_event_at = max((item[0] for item in event_candidates), default=datetime.now(timezone.utc))
    focus_worker = _pick_focus_worker_dict(worker_dicts)
    blocked_workers = sum(1 for worker in worker_dicts if worker["phase"] == WorkerPhase.BLOCKED)
    quiet_workers = sum(1 for worker in worker_dicts if worker["freshness_state"] == "quiet")
    stale_workers = sum(1 for worker in worker_dicts if worker["freshness_state"] == "stale")
    ready_workers = sum(1 for worker in worker_dicts if worker["phase"] == WorkerPhase.HANDOFF_READY)
    active_workers = sum(1 for worker in worker_dicts if worker["phase"] != WorkerPhase.TERMINAL)

    conflict_records = []
    cross_mission_conflicts = 0
    for entry in conflict_index.values():
        sorted_workers = sorted(entry["worker_ids"])
        conflict_records.append(
            {
                "field": entry["field"],
                "value": entry["value"],
                "worker_ids": sorted_workers,
                "is_cross_mission": any(worker_id not in worker_ids for worker_id in sorted_workers),
            }
        )
        if conflict_records[-1]["is_cross_mission"]:
            cross_mission_conflicts += 1

    merge_badges = [
        {"key": state, "label": MERGE_LABELS[state], "count": count}
        for state, count in merge_counts.items()
        if count
    ]
    merge_summary = _build_merge_summary(merge_counts)
    fanout_strip = _build_fanout_strip(worker_dicts)
    timeline = _build_mission_timeline(worker_ids, worker_details, commands)
    exception_badges = _build_exception_badges(
        blocked_workers=blocked_workers,
        stale_workers=stale_workers,
        quiet_workers=quiet_workers,
        ready_workers=ready_workers,
        cross_mission_conflicts=cross_mission_conflicts,
        merge_counts=merge_counts,
    )
    status_label, attention_rank = _derive_mission_status(
        blocked_workers=blocked_workers,
        stale_workers=stale_workers,
        ready_workers=ready_workers,
        active_workers=active_workers,
        worker_count=len(worker_dicts),
        merge_counts=merge_counts,
    )

    mission_id = _mission_id(
        seed_repo,
        branch_hint=branch_hint,
        owner_hint=owner_hint or (focus_worker["worker_id"] if focus_worker else None),
    )
    repo_name = Path(seed_repo).name or seed_repo
    goal = _clip_text(goal_hint or _guess_goal(worker_dicts), 88)

    return {
        "id": mission_id,
        "owner": owner_hint or (focus_worker["worker_id"] if focus_worker else "unassigned"),
        "goal": goal or "Awaiting clearer mission objective",
        "repo_path": seed_repo,
        "repo_name": repo_name,
        "branch_hint": branch_hint,
        "commands": commands,
        "workers": worker_dicts,
        "worker_count": len(worker_dicts),
        "active_workers": active_workers,
        "blocked_workers": blocked_workers,
        "stale_workers": stale_workers,
        "ready_workers": ready_workers,
        "latest_event_at": latest_event_at,
        "latest_event_label": worker_freshness(latest_event_at)["label"],
        "merge_summary": merge_summary,
        "merge_badges": merge_badges,
        "conflicts": sorted(conflict_records, key=lambda item: (item["field"], item["value"])),
        "conflict_count": len(conflict_records),
        "cross_mission_conflicts": cross_mission_conflicts,
        "exception_badges": exception_badges,
        "fanout_strip": fanout_strip,
        "timeline": timeline[:8],
        "focus_worker_id": focus_worker["worker_id"] if focus_worker else None,
        "status_label": status_label,
        "attention_rank": attention_rank,
        "selected": False,
        "worker_search_blob": " ".join(
            filter(
                None,
                [
                    mission_id,
                    seed_repo,
                    branch_hint,
                    owner_hint,
                    goal,
                    " ".join(_worker_search_blob(worker) for worker in worker_dicts),
                    " ".join(command.operator_instruction for command in commands),
                ],
            )
        ).lower(),
        "is_solo": len(worker_dicts) == 1,
        "merge_counts": merge_counts,
    }


def _build_attention_queue(missions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    severity = {
        "blocked": 0,
        "conflict": 1,
        "merge": 2,
        "review": 3,
        "stale": 4,
    }
    for mission in missions:
        age_label = mission["latest_event_label"]
        if mission["blocked_workers"]:
            items.append(
                {
                    "kind": "blocked",
                    "label": "Blocked",
                    "reason": f"{mission['blocked_workers']} worker blocked in {mission['repo_name']}",
                    "age": age_label,
                    "mission_id": mission["id"],
                    "severity": severity["blocked"],
                }
            )
        if mission["cross_mission_conflicts"]:
            items.append(
                {
                    "kind": "conflict",
                    "label": "Conflict",
                    "reason": f"{mission['cross_mission_conflicts']} cross-mission ownership collision",
                    "age": age_label,
                    "mission_id": mission["id"],
                    "severity": severity["conflict"],
                }
            )
        if mission["merge_counts"]["approved"] or mission["merge_counts"]["merged_to_main"]:
            items.append(
                {
                    "kind": "merge",
                    "label": "Merge",
                    "reason": mission["merge_summary"],
                    "age": age_label,
                    "mission_id": mission["id"],
                    "severity": severity["merge"],
                }
            )
        elif mission["ready_workers"]:
            items.append(
                {
                    "kind": "review",
                    "label": "Review",
                    "reason": f"{mission['ready_workers']} worker ready for captain handoff",
                    "age": age_label,
                    "mission_id": mission["id"],
                    "severity": severity["review"],
                }
            )
        if mission["stale_workers"]:
            items.append(
                {
                    "kind": "stale",
                    "label": "Stale",
                    "reason": f"{mission['stale_workers']} worker stale or quiet",
                    "age": age_label,
                    "mission_id": mission["id"],
                    "severity": severity["stale"],
                }
            )
    return sorted(items, key=lambda item: (item["severity"], item["age"], item["mission_id"]))


def _build_summary(missions: list[dict[str, Any]], attention_items: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "missions": len(missions),
        "workers": sum(mission["worker_count"] for mission in missions),
        "attention": len(attention_items),
        "blocked": sum(mission["blocked_workers"] for mission in missions),
        "merge_ready": sum(mission["merge_counts"]["approved"] for mission in missions),
        "done": sum(1 for mission in missions if mission["status_label"] == "done"),
    }


def _build_mission_board(missions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets = {
        "needs attention": [],
        "converging": [],
        "waiting on review": [],
        "done": [],
    }
    for mission in missions:
        label = mission["status_label"]
        if label in {"stuck", "stale"}:
            buckets["needs attention"].append(mission)
        elif label == "waiting on review":
            buckets["waiting on review"].append(mission)
        elif label == "done":
            buckets["done"].append(mission)
        else:
            buckets["converging"].append(mission)
    return [{"label": label, "missions": bucket} for label, bucket in buckets.items()]


def _build_fanout_clusters(missions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    clusters = []
    for mission in missions:
        phase_counts = defaultdict(int)
        for worker in mission["workers"]:
            phase_counts[worker["phase_label"]] += 1
        clusters.append(
            {
                "mission_id": mission["id"],
                "owner": mission["owner"],
                "repo_name": mission["repo_name"],
                "goal": mission["goal"],
                "status_label": mission["status_label"],
                "phase_counts": [
                    {"label": label, "count": count}
                    for label, count in sorted(phase_counts.items(), key=lambda item: item[0])
                ],
                "worker_count": mission["worker_count"],
            }
        )
    return clusters


def _build_dispatch_rows(
    recent_commands: list[OperatorCommandRecord],
    missions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    mission_lookup = {
        (mission["owner"], mission["repo_path"]): mission
        for mission in missions
    }
    rows = []
    for command in recent_commands:
        mission = mission_lookup.get((command.general_worker_id, command.repo_path))
        if mission is None:
            mission = next(
                (item for item in missions if item["owner"] == command.general_worker_id),
                None,
            )
        rows.append(
            {
                "objective": _clip_text(command.operator_instruction, 120),
                "owner": command.general_worker_id,
                "repo_path": command.repo_path,
                "repo_name": Path(command.repo_path).name,
                "branch_hint": command.branch_hint,
                "prompt_path": command.prompt_path,
                "log_path": command.log_path,
                "launch_time": command.created_at,
                "progress": mission["status_label"] if mission else "launched",
                "mission_id": mission["id"] if mission else None,
                "pid": command.pid,
            }
        )
    return rows


def _build_conflict_groups(missions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "mission_id": mission["id"],
            "repo_name": mission["repo_name"],
            "goal": mission["goal"],
            "conflicts": mission["conflicts"],
        }
        for mission in missions
        if mission["conflicts"]
    ]


def _build_worker_rows(missions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for mission in missions:
        for worker in mission["workers"]:
            rows.append(
                {
                    **worker,
                    "mission_id": mission["id"],
                    "mission_goal": mission["goal"],
                    "mission_owner": mission["owner"],
                    "repo_name": mission["repo_name"],
                }
            )
    return sorted(
        rows,
        key=lambda row: (
            PHASE_PRIORITY[row["phase"]],
            row["freshness_state"] != "stale",
            -row["updated_at"].timestamp(),
            row["worker_id"],
        ),
    )


def _apply_search(missions: list[dict[str, Any]], search_query: str) -> list[dict[str, Any]]:
    if not search_query:
        return missions
    return [mission for mission in missions if search_query in mission["worker_search_blob"]]


def _apply_saved_view(missions: list[dict[str, Any]], saved_view: str) -> list[dict[str, Any]]:
    if saved_view == "all-active":
        return [mission for mission in missions if mission["active_workers"] or mission["commands"]]
    if saved_view == "merge-work":
        return [
            mission
            for mission in missions
            if any(
                mission["merge_counts"][state]
                for state in ("pr_open", "changes_requested", "approved", "merged_to_main")
            )
        ]
    if saved_view == "stale-only":
        return [mission for mission in missions if mission["stale_workers"]]
    if saved_view == "solo-workers":
        return [mission for mission in missions if mission["is_solo"]]
    if saved_view == "needs-attention":
        return [
            mission
            for mission in missions
            if mission["blocked_workers"]
            or mission["stale_workers"]
            or mission["conflict_count"]
            or mission["ready_workers"]
            or mission["merge_counts"]["approved"]
        ]
    return missions


def _pick_selected_worker_id(missions: list[dict[str, Any]], requested_worker_id: str | None) -> str | None:
    for mission in missions:
        worker_ids = {worker["worker_id"] for worker in mission["workers"]}
        if requested_worker_id in worker_ids:
            return requested_worker_id
    if missions:
        return missions[0]["focus_worker_id"]
    return None


def _pick_selected_mission_id(
    missions: list[dict[str, Any]],
    *,
    requested_mission_id: str | None,
    requested_worker_id: str | None,
) -> str | None:
    mission_ids = {mission["id"] for mission in missions}
    if requested_mission_id in mission_ids:
        return requested_mission_id
    if requested_worker_id:
        for mission in missions:
            if any(worker["worker_id"] == requested_worker_id for worker in mission["workers"]):
                return mission["id"]
    if missions:
        return missions[0]["id"]
    return None


def _build_mission_timeline(
    worker_ids: list[str],
    worker_details: dict[str, WorkerDetail],
    commands: list[OperatorCommandRecord],
) -> list[dict[str, Any]]:
    events = []
    for command in commands:
        events.append(
            {
                "type": "dispatch",
                "label": "Dispatched",
                "summary": _clip_text(command.operator_instruction, 96),
                "timestamp": command.created_at,
                "worker_id": None,
            }
        )
    for worker_id in worker_ids:
        detail = worker_details[worker_id]
        for transition in detail.transitions:
            events.append(
                {
                    "type": _timeline_type(transition.current_phase, transition.pr_url, transition.status_line, transition.note),
                    "label": _timeline_label(transition.current_phase, transition.pr_url, transition.status_line, transition.note),
                    "summary": _clip_text(transition.note or transition.status_line, 96),
                    "timestamp": transition.created_at,
                    "worker_id": worker_id,
                }
            )
    return sorted(events, key=lambda event: event["timestamp"], reverse=True)


def _timeline_type(
    phase: WorkerPhase,
    pr_url: str | None,
    status_line: str,
    note: str | None,
) -> str:
    text = f"{status_line} {note or ''}".lower()
    if "merged" in text:
        return "merged"
    if pr_url:
        return "pr"
    if phase == WorkerPhase.BLOCKED:
        return "blocked"
    if phase == WorkerPhase.HANDOFF_READY:
        return "review"
    if phase == WorkerPhase.TERMINAL:
        return "terminal"
    return "state"


def _timeline_label(
    phase: WorkerPhase,
    pr_url: str | None,
    status_line: str,
    note: str | None,
) -> str:
    kind = _timeline_type(phase, pr_url, status_line, note)
    if kind == "merged":
        return "Merged"
    if kind == "pr":
        return "PR Opened"
    if kind == "blocked":
        return "Blocked"
    if kind == "review":
        return "Review Requested"
    if kind == "terminal":
        return "Terminal"
    return f"State: {phase.value}"


def _index_conflicts(conflicts: list[ConflictRecord]) -> dict[str, list[ConflictRecord]]:
    indexed: dict[str, list[ConflictRecord]] = defaultdict(list)
    for conflict in conflicts:
        for worker_id in conflict.worker_ids:
            indexed[worker_id].append(conflict)
    return indexed


def _build_fanout_strip(workers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_phase: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for worker in workers:
        by_phase[worker["phase_label"]].append(worker)
    strip = []
    for phase in sorted(by_phase.keys()):
        phase_workers = by_phase[phase]
        strip.append(
            {
                "label": phase,
                "count": len(phase_workers),
                "workers": phase_workers[:4],
                "overflow": max(0, len(phase_workers) - 4),
            }
        )
    return strip


def _build_exception_badges(
    *,
    blocked_workers: int,
    stale_workers: int,
    quiet_workers: int,
    ready_workers: int,
    cross_mission_conflicts: int,
    merge_counts: dict[str, int],
) -> list[dict[str, Any]]:
    badges = []
    if blocked_workers:
        badges.append({"tone": "blocked", "label": f"{blocked_workers} blocked"})
    if cross_mission_conflicts:
        badges.append({"tone": "conflict", "label": f"{cross_mission_conflicts} conflicts"})
    if stale_workers:
        badges.append({"tone": "stale", "label": f"{stale_workers} stale"})
    elif quiet_workers:
        badges.append({"tone": "quiet", "label": f"{quiet_workers} quiet"})
    if ready_workers:
        badges.append({"tone": "review", "label": f"{ready_workers} handoff ready"})
    if merge_counts["approved"]:
        badges.append({"tone": "merge", "label": f"{merge_counts['approved']} approved"})
    return badges


def _derive_mission_status(
    *,
    blocked_workers: int,
    stale_workers: int,
    ready_workers: int,
    active_workers: int,
    worker_count: int,
    merge_counts: dict[str, int],
) -> tuple[str, int]:
    if blocked_workers:
        return "stuck", 0
    if stale_workers:
        return "stale", 1
    if merge_counts["approved"] or merge_counts["changes_requested"] or ready_workers:
        return "waiting on review", 2
    if worker_count and active_workers == 0:
        return "done", 4
    return "converging", 3


def _infer_merge_state(worker: WorkerDetail) -> str:
    text = " ".join(
        filter(
            None,
            [
                worker.status_line,
                worker.blocker,
                worker.last_note.note if worker.last_note else None,
                " ".join(note.note for note in worker.notes[:6]),
            ],
        )
    ).lower()
    if "synced locally" in text or "synced local" in text:
        return "synced_locally"
    if "merged to main" in text or "merged on main" in text or "merged" in text:
        return "merged_to_main"
    if "changes requested" in text or "requested changes" in text:
        return "changes_requested"
    if "approved" in text:
        return "approved"
    if worker.pr_url:
        return "approved" if worker.phase == WorkerPhase.HANDOFF_READY else "pr_open"
    if worker.branch:
        return "branch_active"
    return "no_branch"


def _build_merge_summary(merge_counts: dict[str, int]) -> str:
    parts = []
    for state in ("pr_open", "changes_requested", "approved", "merged_to_main", "synced_locally"):
        count = merge_counts[state]
        if count:
            parts.append(f"{count} {MERGE_LABELS[state]}")
    if parts:
        return ", ".join(parts)
    if merge_counts["branch_active"]:
        return f"{merge_counts['branch_active']} branch active"
    return "no branch activity"


def _pick_focus_worker_dict(workers: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not workers:
        return None
    return sorted(
        workers,
        key=lambda worker: (
            PHASE_PRIORITY[worker["phase"]],
            worker["freshness_state"] != "stale",
            -worker["updated_at"].timestamp(),
            worker["worker_id"],
        ),
    )[0]


def _guess_goal(workers: list[dict[str, Any]]) -> str:
    focus = _pick_focus_worker_dict(workers)
    if not focus:
        return ""
    return focus["next_irreversible_step"] or focus["status_line"]


def _worker_search_blob(worker: dict[str, Any]) -> str:
    return " ".join(
        filter(
            None,
            [
                worker["worker_id"],
                worker["repo_path"],
                worker["branch"],
                worker["owned_artifact"],
                worker["blocker"],
                worker["pr_url"],
                worker["status_line"],
                worker["next_irreversible_step"],
            ],
        )
    )


def _mission_id(repo_path: str, *, branch_hint: str | None, owner_hint: str | None) -> str:
    repo_name = Path(repo_path).name or "repo"
    branch = (branch_hint or owner_hint or "mission").replace("/", "-")
    return f"{repo_name}:{branch}".lower()


def _clip_text(value: str | None, limit: int) -> str:
    if not value:
        return ""
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return f"{compact[: limit - 1].rstrip()}..."


def _titleize(value: str) -> str:
    return value.replace("-", " ").title()


def _age_seconds(value: datetime) -> int:
    now = datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return max(0, int((now - value.astimezone(timezone.utc)).total_seconds()))


def _lineage_root_id(worker: Any, worker_details: dict[str, WorkerDetail]) -> str | None:
    current_id = getattr(worker, "worker_id", None)
    parent_id = getattr(worker, "parent_worker_id", None)
    if getattr(worker, "role", None) in {MemberRole.GENERAL, MemberRole.CAPTAIN} and not parent_id:
        return current_id

    seen: set[str] = set()
    while parent_id and parent_id not in seen:
        seen.add(parent_id)
        parent = worker_details.get(parent_id)
        if parent is None:
            return parent_id if parent_id.startswith(("general-", "captain-")) else None
        if parent.role in {MemberRole.GENERAL, MemberRole.CAPTAIN} and not parent.parent_worker_id:
            return parent.worker_id
        if parent.role == MemberRole.GENERAL:
            return parent.worker_id
        parent_id = parent.parent_worker_id
    return None


def _state_from_status(value: str | None) -> str:
    text = (value or "").lower()
    if "blocked" in text or "stuck" in text:
        return "blocked"
    if "failed" in text:
        return "blocked"
    if "replaced" in text or "terminated" in text:
        return "warn"
    if "lost" in text or "missing" in text or "stale" in text or "terminal" in text:
        return "quiet"
    if "review" in text or "approved" in text or "ready" in text:
        return "warn"
    return "active"


def _detail_messages(worker: WorkerDetail) -> list[dict[str, str]]:
    messages = []
    for message in worker.messages[:12]:
        label = "self" if message.sender_member_id == worker.worker_id else message.sender_role.value
        body = message.body
        if message.related_member_id:
            body = f"{body} ({message.related_member_id})"
        messages.append(
            {
                "sender": label,
                "type": message.message_type,
                "body": body,
                "age": format_relative_time(message.created_at),
            }
        )
    return messages


def _extended_detail_items(worker: WorkerDetail) -> list[dict[str, str]]:
    return [
        {"label": "Role", "value": worker.role.value},
        {"label": "Current State", "value": worker.effective_state},
        {"label": "Phase", "value": SUPERVISION_PHASE_LABELS[worker.phase]},
        {"label": "Repo", "value": Path(worker.repo_path).name or worker.repo_path},
        {"label": "Branch", "value": worker.branch or "none"},
        {"label": "Updated", "value": format_relative_time(worker.updated_at)},
        {"label": "Parent", "value": worker.parent_worker_id or "missing"},
        {"label": "Process", "value": str(worker.process_id) if worker.process_id else "none"},
        {
            "label": "Heartbeat",
            "value": (
                f"{worker.last_heartbeat.observed_state} @ {format_relative_time(worker.last_heartbeat.created_at)}"
                if worker.last_heartbeat
                else "none"
            ),
        },
        {
            "label": "Self Report",
            "value": (
                f"{worker.phase.value} @ {format_relative_time(worker.last_self_reported_at)}"
                if worker.last_self_reported_at
                else "none"
            ),
        },
        {
            "label": "Parent Report",
            "value": (
                f"{worker.last_parent_report.observed_status_line} @ {format_relative_time(worker.last_parent_report.created_at)}"
                if worker.last_parent_report
                else "none"
            ),
        },
        {"label": "Recovery", "value": _replacement_label(worker.last_parent_report)},
        {"label": "Next", "value": worker.next_irreversible_step or "none"},
        {"label": "Note", "value": worker.last_note.note if worker.last_note else "none"},
    ]


def _build_worker_graph_node(
    worker: WorkerDetail,
) -> dict[str, Any]:
    actions = [
        {"label": "Focus worker", "href": f"/?worker={worker.worker_id}"},
    ]
    if worker.pr_url:
        actions.append({"label": "Open PR", "href": worker.pr_url})
    return {
        "id": _member_node_id(worker.worker_id, worker.role),
        "role": worker.role.value,
        "display_type": worker.role.value,
        "status_label": _node_status_label(worker.effective_state or worker.phase.value),
        "age_label": _short_age_label(worker.updated_at),
        "state": _state_from_status(worker.effective_state or worker.phase.value),
        "detail": {
            "title": worker.worker_id,
            "subtitle": worker.role.value,
            "state": worker.effective_state,
            "mission": worker.status_line,
            "messages": _detail_messages(worker),
            "extended": _extended_detail_items(worker),
            "actions": actions,
        },
    }


def _build_root_graph_node(
    root_worker_id: str,
    detail: WorkerDetail | None,
    command: OperatorCommandRecord | None,
) -> dict[str, Any]:
    if command is not None and command.dispatch_role == DispatchRole.CAPTAIN and detail is None:
        return _build_dispatched_captain_graph_node(root_worker_id, command)
    if detail is not None:
        node = _build_worker_graph_node(detail)
        node["id"] = _command_root_node_id(root_worker_id, command)
        node["role"] = detail.role.value
        node["display_type"] = detail.role.value
        node["detail"]["subtitle"] = detail.role.value
        if command is not None:
            node["detail"]["extended"].extend(
                [
                    {"label": "Mission", "value": _clip_text(command.operator_instruction, 160) or "none"},
                    {"label": "Dispatch Log", "value": command.log_path},
                ]
            )
        return node

    status_label = "launched" if command is not None else "active"
    created_at = command.created_at if command is not None else datetime.now(timezone.utc)
    return {
        "id": f"general:{root_worker_id}",
        "role": "general",
        "display_type": "general",
        "status_label": _node_status_label(status_label),
        "age_label": _short_age_label(created_at),
        "state": _state_from_status(status_label),
        "detail": {
            "title": root_worker_id,
            "subtitle": "general",
            "state": status_label,
            "mission": _clip_text(command.operator_instruction if command else "dispatched general", 160),
            "messages": [],
            "extended": [
                {"label": "Current State", "value": status_label},
                {"label": "Repo", "value": Path(command.repo_path).name if command else "none"},
                {"label": "Mission", "value": command.operator_instruction if command else "none"},
                {"label": "Dispatch PID", "value": str(command.pid) if command else "none"},
                {"label": "Dispatch Log", "value": command.log_path if command else "none"},
            ],
            "actions": [],
        },
    }


def _build_dispatched_captain_graph_node(
    captain_worker_id: str,
    command: OperatorCommandRecord,
) -> dict[str, Any]:
    return {
        "id": f"worker:{captain_worker_id}",
        "role": "captain",
        "display_type": "captain",
        "status_label": _node_status_label("launched"),
        "age_label": _short_age_label(command.created_at),
        "state": _state_from_status("launched"),
        "detail": {
            "title": captain_worker_id,
            "subtitle": "captain",
            "state": "launched",
            "mission": _clip_text(command.operator_instruction, 160),
            "messages": [],
            "extended": [
                {"label": "Current State", "value": "launched"},
                {"label": "Repo", "value": Path(command.repo_path).name or command.repo_path},
                {"label": "Mission", "value": command.operator_instruction},
                {"label": "Dispatch PID", "value": str(command.pid)},
                {"label": "Dispatch Log", "value": command.log_path},
            ],
            "actions": [],
        },
    }


def _command_root_node_id(root_worker_id: str, command: OperatorCommandRecord | None) -> str:
    if command is not None and command.dispatch_role == DispatchRole.CAPTAIN:
        return f"worker:{root_worker_id}"
    return f"general:{root_worker_id}"


def _selected_root_node_id(
    selected_worker_id: str,
    latest_command_by_root: dict[str, OperatorCommandRecord],
) -> str:
    command = latest_command_by_root.get(selected_worker_id)
    return _command_root_node_id(selected_worker_id, command)


def _member_node_id(worker_id: str, role: MemberRole) -> str:
    if role == MemberRole.GENERAL:
        return f"general:{worker_id}"
    return f"worker:{worker_id}"


def _graph_role_order(role: MemberRole) -> int:
    if role == MemberRole.GENERAL:
        return 0
    if role == MemberRole.CAPTAIN:
        return 1
    return 2


def _resolve_graph_parent_id(
    parent_worker_id: str | None,
    worker_details: dict[str, WorkerDetail],
    latest_command_by_root: dict[str, OperatorCommandRecord],
) -> str | None:
    if not parent_worker_id:
        return None
    parent_detail = worker_details.get(parent_worker_id)
    if parent_detail is not None:
        return _member_node_id(parent_detail.worker_id, parent_detail.role)
    parent_command = latest_command_by_root.get(parent_worker_id)
    if parent_command is not None:
        return _command_root_node_id(parent_worker_id, parent_command)
    return None


def _ensure_orphan_worker_cluster(
    worker: WorkerDetail,
    nodes: list[dict[str, Any]],
    seen_ids: set[str],
    orphan_worker_clusters: dict[str, str],
) -> str:
    cluster_key = worker.repo_path or worker.worker_id
    cluster_id = orphan_worker_clusters.get(cluster_key)
    if cluster_id is not None:
        return cluster_id
    slug = "".join(character if character.isalnum() else "-" for character in cluster_key.lower()).strip("-")
    cluster_id = f"captain:orphan:{slug or 'unknown'}"
    orphan_worker_clusters[cluster_key] = cluster_id
    if cluster_id not in seen_ids:
        nodes.append(
            {
                "id": cluster_id,
                "role": "captain",
                "display_type": "captain",
                "status_label": "attention",
                "age_label": _short_age_label(worker.updated_at),
                "state": "warn",
                "detail": {
                    "title": Path(worker.repo_path).name or worker.repo_path,
                    "subtitle": "synthetic captain",
                    "state": "attention",
                    "mission": "Synthetic wrapper for unparented workers. Fix parent_worker_id at registration.",
                    "messages": [],
                    "extended": [
                        {"label": "Repo", "value": worker.repo_path},
                        {"label": "Lineage", "value": "missing captain parent"},
                    ],
                    "actions": [],
                },
            }
        )
        seen_ids.add(cluster_id)
    return cluster_id


def _node_status_label(value: str | None) -> str:
    text = (value or "").lower()
    if "blocked" in text or "stuck" in text:
        return "blocked"
    if "failed" in text:
        return "blocked"
    if "replaced" in text:
        return "replaced"
    if "terminated" in text:
        return "terminated"
    if "complete" in text:
        return "done"
    if "lost" in text or "missing" in text:
        return "stale"
    if "stale" in text:
        return "stale"
    if "quiet" in text:
        return "quiet"
    if "review" in text or "approved" in text or "ready" in text:
        return "attention"
    if "terminal" in text or "done" in text:
        return "done"
    return "active"


def _short_age_label(value: datetime) -> str:
    delta_seconds = _age_seconds(value)
    if delta_seconds < 60:
        return f"{delta_seconds}s"
    if delta_seconds < 3600:
        return f"{max(1, delta_seconds // 60)}m"
    if delta_seconds < 86400:
        return f"{max(1, delta_seconds // 3600)}h"
    return f"{max(1, delta_seconds // 86400)}d"


def _replacement_label(report: ParentReportRecord | None) -> str:
    if report is None or not report.event_type:
        return "none"
    related = report.related_member_id or "none"
    if report.event_type == "replaced_underling":
        return f"replaced by {related}"
    if report.event_type == "terminated_underling":
        return f"terminated {related}"
    if report.event_type == "spawned_underling":
        return f"spawned {related}"
    return report.event_type
