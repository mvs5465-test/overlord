import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib import request

from fastapi.testclient import TestClient

from overlord.app import create_app
from overlord.config import Settings
from overlord.dashboard import build_supervision_view
from overlord.dispatcher import CodexDispatcher, DEFAULT_CODEX_MODEL
from overlord.models import DispatchStatus, OperatorCommandCreate, OperatorCommandLaunch
from overlord.store import StateStore
from overlord.worker_status import (
    build_member_message_payload,
    build_parent_report_payload,
    build_parser,
    build_payload,
    build_registration_payload,
)


class FakeDispatcher:
    def __init__(self) -> None:
        self.commands: list[OperatorCommandCreate] = []

    def dispatch(self, command: OperatorCommandCreate) -> OperatorCommandLaunch:
        self.commands.append(command)
        return OperatorCommandLaunch(
            status=DispatchStatus.LAUNCHED,
            pid=4242,
            prompt_path=str(Path(command.repo_path) / "dispatch.prompt.txt"),
            log_path=str(Path(command.repo_path) / "dispatch.log"),
        )


def build_client(tmp_path: Path, dispatcher: FakeDispatcher | None = None) -> TestClient:
    settings = Settings(
        OVERLORD_APP_NAME="Overlord Test",
        OVERLORD_DEFAULT_ENVIRONMENT="test",
        OVERLORD_DEFAULT_WORKSPACE="sandbox",
        OVERLORD_DATA_DIR=tmp_path,
        OVERLORD_ALLOWED_REPO_ROOTS=str(tmp_path),
    )
    return TestClient(create_app(settings, dispatcher=dispatcher))


def register_worker(client: TestClient, repo_path: Path) -> None:
    response = client.post(
        "/api/workers/events",
        json={
            "worker_id": "worker-123",
            "worker_token": "secret-worker-token",
            "current_phase": "assigned",
            "previous_phase": None,
            "repo_path": str(repo_path),
            "branch": "feat/control-plane-mvp",
            "worktree": str(repo_path / "worktree-1"),
            "owned_artifact": "overlord/app.py",
            "status_line": "claiming backend slice",
            "note": "starting in assigned before repo scouting",
        },
    )
    assert response.status_code == 201


def register_general(
    client: TestClient,
    repo_path: Path,
    *,
    worker_id: str = "general-123",
    worker_token: str = "general-secret-token",
) -> None:
    response = client.post(
        "/api/workers/events",
        json={
            "worker_id": worker_id,
            "worker_token": worker_token,
            "role": "general",
            "current_phase": "assigned",
            "previous_phase": None,
            "repo_path": str(repo_path),
            "branch": "feat/control-plane-mvp",
            "status_line": "general standing up mission control",
        },
    )
    assert response.status_code == 201


def transition_worker(
    client: TestClient,
    repo_path: Path,
    *,
    phase: str,
    previous_phase: str,
    status_line: str,
    next_step: str | None = None,
    note: str | None = None,
    blocker: str | None = None,
) -> None:
    payload = {
        "worker_id": "worker-123",
        "worker_token": "secret-worker-token",
        "current_phase": phase,
        "previous_phase": previous_phase,
        "repo_path": str(repo_path),
        "branch": "feat/control-plane-mvp",
        "worktree": str(repo_path / "worktree-1"),
        "owned_artifact": "overlord/app.py",
        "status_line": status_line,
    }
    if next_step:
        payload["next_irreversible_step"] = next_step
    if note:
        payload["note"] = note
    if blocker:
        payload["blocker"] = blocker

    response = client.post("/api/workers/events", json=payload)
    assert response.status_code == 201


def test_healthz(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["db_path"] == str(tmp_path / "overlord.db")
    assert response.json()["workers"] == 0


def test_healthz_returns_503_when_store_healthcheck_fails(tmp_path: Path) -> None:
    client = build_client(tmp_path)

    def broken_healthcheck() -> dict[str, object]:
        raise RuntimeError("db unavailable")

    client.app.state.store.healthcheck = broken_healthcheck  # type: ignore[method-assign]

    response = client.get("/healthz")

    assert response.status_code == 503
    assert response.json() == {"status": "error", "detail": "db unavailable"}


def test_meta_endpoint_exposes_control_plane_defaults(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    response = client.get("/api/meta")

    assert response.status_code == 200
    assert response.json()["mode"] == "control-plane-mvp"
    assert response.json()["defaults"]["environment"] == "test"
    assert response.json()["defaults"]["workspace"] == "sandbox"
    assert response.json()["api"]["events"] == "/api/workers/events"
    assert response.json()["api"]["commands"] == "/api/commands"


def test_homepage_renders_live_dashboard(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    register_worker(client, tmp_path)
    transition_worker(
        client,
        tmp_path,
        phase="scouting",
        previous_phase="assigned",
        status_line="reading repo instructions and app shape",
        next_step="lock the ui artifact boundary",
        note="reviewed app, store, and current operator board",
    )
    transition_worker(
        client,
        tmp_path,
        phase="planned",
        previous_phase="scouting",
        status_line="scoped the server-rendered ui slice",
        next_step="patch template and styles together",
        note="keeping api and persistence untouched",
    )

    response = client.get("/")

    assert response.status_code == 200
    assert "Overlord Test" in response.text
    assert "graph-stage" in response.text
    assert "worker-123" in response.text
    assert "/api/graph" in response.text
    assert "keeping api and persistence untouched" in response.text


def test_selected_worker_is_reflected_in_graph_popup(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    register_worker(client, tmp_path)
    transition_worker(
        client,
        tmp_path,
        phase="scouting",
        previous_phase="assigned",
        status_line="reading repo instructions and app shape",
        next_step="lock the ui artifact boundary",
    )

    response = client.get("/?worker=worker-123")

    assert response.status_code == 200
    assert "worker-123" in response.text
    assert "lock the ui artifact boundary" in response.text


def test_search_and_saved_view_keep_mission_focus(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    register_worker(client, tmp_path)
    transition_worker(
        client,
        tmp_path,
        phase="scouting",
        previous_phase="assigned",
        status_line="reading repo instructions and app shape",
        next_step="map mission grouping",
        note="status is fresh and should appear in all active",
    )
    client.post(
        "/api/workers/events",
        json={
            "worker_id": "worker-456",
            "worker_token": "second-secret-token",
            "current_phase": "assigned",
            "previous_phase": None,
            "repo_path": str(tmp_path / "second-repo"),
            "status_line": "waiting in another repo",
        },
    )

    response = client.get("/?view=missions&saved_view=all-active&q=worker-123")

    assert response.status_code == 200
    assert "worker-123" in response.text
    assert "worker-123" in response.text


def test_merge_and_conflict_signals_render_in_mission_view(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    register_worker(client, tmp_path)
    transition_worker(
        client,
        tmp_path,
        phase="scouting",
        previous_phase="assigned",
        status_line="reading repo instructions and app shape",
        next_step="open a PR for review",
    )
    client.post(
        "/api/workers/events",
        json={
            "worker_id": "worker-789",
            "worker_token": "third-secret-token",
            "current_phase": "assigned",
            "previous_phase": None,
            "repo_path": str(tmp_path),
            "branch": "feat/control-plane-mvp",
            "worktree": str(tmp_path / "worktree-2"),
            "owned_artifact": "overlord/app.py",
            "status_line": "approved and ready to merge",
            "pr_url": "https://github.com/mvs5465-test/overlord/pull/9",
        },
    )

    response = client.get("/?view=conflicts&saved_view=all-active")

    assert response.status_code == 200
    assert "approved" in response.text
    assert "feat/control-plane-mvp" in response.text


def test_worker_event_persists_and_project_current_state(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    register_worker(client, tmp_path)

    transition = client.post(
        "/api/workers/events",
        json={
            "worker_id": "worker-123",
            "worker_token": "secret-worker-token",
            "current_phase": "scouting",
            "previous_phase": "assigned",
            "repo_path": str(tmp_path),
            "branch": "feat/control-plane-mvp",
            "worktree": str(tmp_path / "worktree-1"),
            "owned_artifact": "overlord/app.py",
            "status_line": "reading repo instructions and app shape",
            "next_irreversible_step": "write sqlite-backed store",
            "note": "local instructions loaded and branch boundary is clear",
        },
    )

    assert transition.status_code == 201

    worker_response = client.get("/api/workers/worker-123")
    assert worker_response.status_code == 200
    worker = worker_response.json()["worker"]
    assert worker["phase"] == "scouting"
    assert worker["transitions"][0]["current_phase"] == "scouting"
    assert worker["notes"][0]["note"] == "local instructions loaded and branch boundary is clear"

    list_response = client.get("/api/workers")
    assert list_response.status_code == 200
    assert list_response.json()["workers"][0]["worker_id"] == "worker-123"


def test_invalid_transition_is_rejected(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    register_worker(client, tmp_path)

    response = client.post(
        "/api/workers/events",
        json={
            "worker_id": "worker-123",
            "worker_token": "secret-worker-token",
            "current_phase": "validating",
            "previous_phase": "assigned",
            "repo_path": str(tmp_path),
            "status_line": "skipping ahead",
            "next_irreversible_step": "run pytest",
        },
    )

    assert response.status_code == 409
    assert "not allowed" in response.json()["detail"]


def test_repo_allowlist_is_enforced(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    response = client.post(
        "/api/workers/events",
        json={
            "worker_id": "worker-456",
            "worker_token": "another-secret",
            "current_phase": "assigned",
            "previous_phase": None,
            "repo_path": "/tmp/not-allowed",
            "status_line": "trying outside configured roots",
        },
    )

    assert response.status_code == 422
    assert "allowed roots" in response.json()["detail"]


def test_worker_note_endpoint_requires_matching_token(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    register_worker(client, tmp_path)

    forbidden = client.post(
        "/api/workers/worker-123/notes",
        json={
            "worker_token": "wrong-token",
            "phase": "assigned",
            "note": "should not be accepted",
        },
    )

    assert forbidden.status_code == 403

    accepted = client.post(
        "/api/workers/worker-123/notes",
        json={
            "worker_token": "secret-worker-token",
            "phase": "assigned",
            "note": "scout note recorded separately",
        },
    )

    assert accepted.status_code == 201
    assert accepted.json()["note"]["note"] == "scout note recorded separately"


def test_conflicts_are_reported_for_shared_branch(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    register_worker(client, tmp_path)
    client.post(
        "/api/workers/events",
        json={
            "worker_id": "worker-456",
            "worker_token": "token-456789",
            "current_phase": "assigned",
            "previous_phase": None,
            "repo_path": str(tmp_path),
            "branch": "feat/control-plane-mvp",
            "worktree": str(tmp_path / "worktree-2"),
            "owned_artifact": "overlord/templates/index.html",
            "status_line": "claiming ui slice",
        },
    )

    response = client.get("/api/workers")

    assert response.status_code == 200
    conflicts = response.json()["conflicts"]
    assert any(conflict["field"] == "branch" for conflict in conflicts)


def test_homepage_focuses_requested_worker_in_control_pane(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    register_worker(client, tmp_path)
    client.post(
        "/api/workers/events",
        json={
            "worker_id": "worker-456",
            "worker_token": "token-456789",
            "current_phase": "assigned",
            "previous_phase": None,
            "repo_path": str(tmp_path),
            "branch": "feat/ui-slice",
            "worktree": str(tmp_path / "worktree-2"),
            "owned_artifact": "overlord/templates/index.html",
            "status_line": "claiming ui slice",
            "note": "ready to build the operator surface",
        },
    )

    response = client.get("/?worker=worker-456")

    assert response.status_code == 200
    assert "claiming ui slice" in response.text
    assert "ready to build the operator surface" in response.text


def test_dashboard_self_report_form_registers_worker(tmp_path: Path) -> None:
    client = build_client(tmp_path)

    response = client.post(
        "/report",
        data={
            "worker_id": "worker-ui-1",
            "worker_token": "worker-ui-secret",
            "current_phase": "assigned",
            "previous_phase": "",
            "repo_path": str(tmp_path),
            "status_line": "checking in from the dashboard form",
            "note": "manual worker report path works",
            "branch": "feat/ui-report",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    follow_up = client.get(response.headers["location"])
    assert follow_up.status_code == 200
    assert "worker-ui-1" in follow_up.text
    assert "manual worker report path works" in follow_up.text


def test_dashboard_self_report_form_surfaces_validation_error(tmp_path: Path) -> None:
    client = build_client(tmp_path)

    response = client.post(
        "/report",
        data={
            "worker_id": "worker-ui-2",
            "worker_token": "worker-ui-secret",
            "current_phase": "blocked",
            "previous_phase": "",
            "repo_path": str(tmp_path),
            "status_line": "trying to post a blocked transition",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert "blocked%20transitions%20must%20include%20a%20blocker" in response.headers["location"] or "blocked+transitions+must+include+a+blocker" in response.headers["location"]


def test_command_api_launches_general_prompt_and_persists_order(tmp_path: Path) -> None:
    dispatcher = FakeDispatcher()
    client = build_client(tmp_path, dispatcher=dispatcher)

    response = client.post(
        "/api/commands",
        json={
            "general_worker_id": "general-local-1",
            "repo_path": str(tmp_path),
            "branch_hint": "feat/localhost-mvp",
            "operator_instruction": "Finish the localhost MVP and report back with tests.",
        },
    )

    assert response.status_code == 201
    command = response.json()["command"]
    assert command["general_worker_id"] == "general-local-1"
    assert command["status"] == "launched"
    assert command["pid"] == 4242
    assert dispatcher.commands[0].operator_instruction.startswith("Finish the localhost MVP")

    list_response = client.get("/api/commands")
    assert list_response.status_code == 200
    assert list_response.json()["commands"][0]["general_worker_id"] == "general-local-1"
    assert list_response.json()["commands"][0]["dispatch_role"] == "general"


def test_command_api_launches_captain_prompt_and_persists_role(tmp_path: Path) -> None:
    dispatcher = FakeDispatcher()
    client = build_client(tmp_path, dispatcher=dispatcher)

    response = client.post(
        "/api/commands",
        json={
            "general_worker_id": "captain-local-1",
            "dispatch_role": "captain",
            "repo_path": str(tmp_path),
            "branch_hint": "feat/point-fix",
            "operator_instruction": "Handle this focused point fix directly as captain.",
        },
    )

    assert response.status_code == 201
    command = response.json()["command"]
    assert command["general_worker_id"] == "captain-local-1"
    assert command["dispatch_role"] == "captain"
    assert dispatcher.commands[0].dispatch_role == "captain"


def test_codex_dispatcher_uses_explicit_gpt_5_4_model_and_general_prompt_instructs_same(tmp_path: Path, monkeypatch) -> None:
    settings = Settings(
        OVERLORD_APP_NAME="Overlord Test",
        OVERLORD_DEFAULT_ENVIRONMENT="test",
        OVERLORD_DEFAULT_WORKSPACE="sandbox",
        OVERLORD_DATA_DIR=tmp_path,
        OVERLORD_ALLOWED_REPO_ROOTS=str(tmp_path),
    )
    dispatcher = CodexDispatcher(settings)
    captured: dict[str, object] = {}

    class FakeProcess:
        pid = 5150

    def fake_popen(args, **kwargs):  # type: ignore[no-untyped-def]
        captured["args"] = args
        captured["cwd"] = kwargs["cwd"]
        prompt_bytes = kwargs["stdin"].read()
        captured["prompt"] = prompt_bytes.decode("utf-8")
        return FakeProcess()

    monkeypatch.setattr("overlord.dispatcher.subprocess.Popen", fake_popen)

    launch = dispatcher.dispatch(
        OperatorCommandCreate(
            general_worker_id="general-local-1",
            repo_path=str(tmp_path),
            branch_hint="feat/localhost-mvp",
            operator_instruction="Finish the localhost MVP and report back with tests.",
        )
    )

    assert launch.pid == 5150
    assert captured["cwd"] == str(tmp_path)
    assert captured["args"][:4] == ["codex", "exec", "-m", DEFAULT_CODEX_MODEL]
    assert f"launch every captain with `codex exec -m {DEFAULT_CODEX_MODEL}`" in captured["prompt"]


def test_codex_dispatcher_builds_direct_captain_prompt(tmp_path: Path, monkeypatch) -> None:
    settings = Settings(
        OVERLORD_APP_NAME="Overlord Test",
        OVERLORD_DEFAULT_ENVIRONMENT="test",
        OVERLORD_DEFAULT_WORKSPACE="sandbox",
        OVERLORD_DATA_DIR=tmp_path,
        OVERLORD_ALLOWED_REPO_ROOTS=str(tmp_path),
    )
    dispatcher = CodexDispatcher(settings)
    captured: dict[str, object] = {}

    class FakeProcess:
        pid = 6161

    def fake_popen(args, **kwargs):  # type: ignore[no-untyped-def]
        captured["args"] = args
        captured["prompt"] = kwargs["stdin"].read().decode("utf-8")
        return FakeProcess()

    monkeypatch.setattr("overlord.dispatcher.subprocess.Popen", fake_popen)

    launch = dispatcher.dispatch(
        OperatorCommandCreate(
            general_worker_id="captain-local-1",
            dispatch_role="captain",
            repo_path=str(tmp_path),
            branch_hint="feat/point-fix",
            operator_instruction="Handle this focused point fix directly as captain.",
        )
    )

    assert launch.pid == 6161
    assert captured["args"][:4] == ["codex", "exec", "-m", DEFAULT_CODEX_MODEL]
    assert captured["prompt"].startswith("$codex-captain")
    assert "role: captain" in captured["prompt"]
    assert f"launch every worker with `codex exec -m {DEFAULT_CODEX_MODEL}`" in captured["prompt"]


def test_member_registration_creates_captain_with_parent_and_process_metadata(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    register_general(client, tmp_path)

    response = client.post(
        "/api/members/register",
        json={
            "member_id": "captain-123",
            "member_token": "captain-secret-token",
            "role": "captain",
            "parent_member_id": "general-123",
            "parent_token": "general-secret-token",
            "repo_path": str(tmp_path),
            "branch": "feat/control-plane-mvp",
            "host_id": "localhost",
            "process_id": 99991,
            "process_started_at": "2026-03-15T18:00:00Z",
            "phase": "assigned",
            "status_line": "registered by general",
            "note": "captain spawned and attached",
        },
    )

    assert response.status_code == 201
    member = response.json()["member"]
    assert member["worker_id"] == "captain-123"
    assert member["role"] == "captain"
    assert member["parent_worker_id"] == "general-123"
    assert member["process_id"] == 99991


def test_captain_registration_requires_parent_member_id(tmp_path: Path) -> None:
    client = build_client(tmp_path)

    response = client.post(
        "/api/members/register",
        json={
            "member_id": "captain-orphan-1",
            "member_token": "captain-secret-token",
            "role": "captain",
            "repo_path": str(tmp_path),
            "phase": "assigned",
            "status_line": "registered without lineage",
        },
    )

    assert response.status_code == 422
    assert "captain members must declare a parent" in response.text


def test_parent_report_is_stored_and_reflected_on_member_detail(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    register_general(client, tmp_path)
    client.post(
        "/api/members/register",
        json={
            "member_id": "captain-123",
            "member_token": "captain-secret-token",
            "role": "captain",
            "parent_member_id": "general-123",
            "parent_token": "general-secret-token",
            "repo_path": str(tmp_path),
            "phase": "assigned",
            "status_line": "registered by general",
        },
    )

    response = client.post(
        "/api/members/captain-123/parent-report",
        json={
            "subject_member_id": "captain-123",
            "reporter_member_id": "general-123",
            "reporter_token": "general-secret-token",
            "event_type": "terminated_underling",
            "related_member_id": "worker-bad-1",
            "observed_phase": "blocked",
            "observed_status_line": "captain stopped responding",
            "observed_state": "missing",
            "note": "general escalation",
            "process_id": 12345,
        },
    )

    assert response.status_code == 201
    detail = client.get("/api/workers/captain-123").json()["worker"]
    assert detail["last_parent_report"]["observed_state"] == "missing"
    assert detail["parent_reports"][0]["reporter_member_id"] == "general-123"
    assert detail["parent_reports"][0]["event_type"] == "terminated_underling"
    assert detail["parent_reports"][0]["related_member_id"] == "worker-bad-1"


def test_member_message_is_stored_and_reflected_on_member_detail(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    register_general(client, tmp_path)
    client.post(
        "/api/members/register",
        json={
            "member_id": "captain-123",
            "member_token": "captain-secret-token",
            "role": "captain",
            "parent_member_id": "general-123",
            "parent_token": "general-secret-token",
            "repo_path": str(tmp_path),
            "phase": "assigned",
            "status_line": "registered by general",
        },
    )

    response = client.post(
        "/api/members/captain-123/messages",
        json={
            "member_id": "captain-123",
            "sender_member_id": "general-123",
            "sender_token": "general-secret-token",
            "message_type": "check",
            "body": "checked in and captain is on mission",
        },
    )

    assert response.status_code == 201
    detail = client.get("/api/workers/captain-123").json()["worker"]
    assert detail["last_message"]["message_type"] == "check"
    assert detail["messages"][0]["sender_member_id"] == "general-123"
    assert detail["messages"][0]["body"] == "checked in and captain is on mission"


def test_member_message_rejects_non_parent_sender(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    register_general(client, tmp_path, worker_id="general-123", worker_token="general-secret-token")
    register_general(client, tmp_path, worker_id="general-999", worker_token="general-other-token")
    client.post(
        "/api/members/register",
        json={
            "member_id": "captain-123",
            "member_token": "captain-secret-token",
            "role": "captain",
            "parent_member_id": "general-123",
            "parent_token": "general-secret-token",
            "repo_path": str(tmp_path),
            "phase": "assigned",
            "status_line": "registered by general",
        },
    )

    response = client.post(
        "/api/members/captain-123/messages",
        json={
            "member_id": "captain-123",
            "sender_member_id": "general-999",
            "sender_token": "general-other-token",
            "message_type": "check",
            "body": "illegitimate supervisor message",
        },
    )

    assert response.status_code == 403
    assert "direct child" in response.json()["detail"]


def test_graph_attaches_captain_to_dispatched_general_using_parent_id(tmp_path: Path) -> None:
    dispatcher = FakeDispatcher()
    client = build_client(tmp_path, dispatcher=dispatcher)

    command_response = client.post(
        "/api/commands",
        json={
            "general_worker_id": "general-overlord-1",
            "repo_path": str(tmp_path),
            "branch_hint": "feat/overlord",
            "operator_instruction": "coordinate the ideas mission",
        },
    )
    assert command_response.status_code == 201

    registration_response = client.post(
        "/api/members/register",
        json={
            "member_id": "captain-general-overlord-1-ideas-001",
            "member_token": "captain-secret-token",
            "role": "captain",
            "parent_member_id": "general-overlord-1",
            "repo_path": str(tmp_path / "ideas"),
            "branch": "feat/ideas",
            "host_id": "localhost",
            "process_id": 99123,
            "process_started_at": "2026-03-15T18:00:00Z",
            "phase": "assigned",
            "status_line": "registered by general under a child repo",
        },
    )
    assert registration_response.status_code == 201

    graph_response = client.get("/api/graph")
    assert graph_response.status_code == 200
    graph = graph_response.json()["graph"]
    nodes = {node["id"]: node for node in graph["nodes"]}
    edges = {(edge["source"], edge["target"]) for edge in graph["edges"]}

    assert "general:general-overlord-1" in nodes
    assert "worker:captain-general-overlord-1-ideas-001" in nodes
    assert ("overlord", "general:general-overlord-1") in edges
    assert ("general:general-overlord-1", "worker:captain-general-overlord-1-ideas-001") in edges
    assert [node_id for node_id, node in nodes.items() if node["role"] == "general"] == ["general:general-overlord-1"]


def test_graph_renders_direct_captain_dispatch_under_overlord(tmp_path: Path) -> None:
    dispatcher = FakeDispatcher()
    client = build_client(tmp_path, dispatcher=dispatcher)

    command_response = client.post(
        "/api/commands",
        json={
            "general_worker_id": "captain-overlord-1",
            "dispatch_role": "captain",
            "repo_path": str(tmp_path),
            "branch_hint": "feat/point-fix",
            "operator_instruction": "Handle this focused point fix directly as captain.",
        },
    )
    assert command_response.status_code == 201

    graph_response = client.get("/api/graph")
    assert graph_response.status_code == 200
    graph = graph_response.json()["graph"]
    nodes = {node["id"]: node for node in graph["nodes"]}
    edges = {(edge["source"], edge["target"]) for edge in graph["edges"]}

    assert "worker:captain-overlord-1" in nodes
    assert nodes["worker:captain-overlord-1"]["role"] == "captain"
    assert ("overlord", "worker:captain-overlord-1") in edges


def test_supervision_groups_child_repo_captain_under_explicit_general_lineage(tmp_path: Path) -> None:
    dispatcher = FakeDispatcher()
    client = build_client(tmp_path, dispatcher=dispatcher)

    command_response = client.post(
        "/api/commands",
        json={
            "general_worker_id": "general-overlord-1",
            "repo_path": str(tmp_path),
            "branch_hint": "feat/overlord",
            "operator_instruction": "coordinate the ideas mission",
        },
    )
    assert command_response.status_code == 201

    registration_response = client.post(
        "/api/members/register",
        json={
            "member_id": "captain-general-overlord-1-ideas-001",
            "member_token": "captain-secret-token",
            "role": "captain",
            "parent_member_id": "general-overlord-1",
            "repo_path": str(tmp_path / "ideas"),
            "branch": "feat/ideas",
            "phase": "assigned",
            "status_line": "registered by general under a child repo",
        },
    )
    assert registration_response.status_code == 201

    store = StateStore(tmp_path)
    snapshot = store.snapshot()
    worker_details = {worker.worker_id: store.get_worker(worker.worker_id) for worker in snapshot.workers}
    worker_states = {worker.worker_id: {"state": "active", "label": "active now", "age_minutes": 0} for worker in snapshot.workers}
    supervision = build_supervision_view(
        snapshot,
        worker_details,
        store.list_commands(),
        requested_worker_id=None,
        requested_mission_id=None,
        search_query="",
        current_view="missions",
        saved_view="all-active",
    )

    assert len(supervision["mission_rows"]) == 1
    mission = supervision["mission_rows"][0]
    assert mission["owner"] == "general-overlord-1"
    assert mission["repo_path"] == str(tmp_path)
    assert mission["workers"][0]["worker_id"] == "captain-general-overlord-1-ideas-001"


def test_supervision_groups_direct_captain_dispatch_as_root_mission(tmp_path: Path) -> None:
    dispatcher = FakeDispatcher()
    client = build_client(tmp_path, dispatcher=dispatcher)

    command_response = client.post(
        "/api/commands",
        json={
            "general_worker_id": "captain-overlord-1",
            "dispatch_role": "captain",
            "repo_path": str(tmp_path),
            "branch_hint": "feat/point-fix",
            "operator_instruction": "Handle this focused point fix directly as captain.",
        },
    )
    assert command_response.status_code == 201

    registration_response = client.post(
        "/api/workers/events",
        json={
            "worker_id": "captain-overlord-1",
            "worker_token": "captain-secret-token",
            "role": "captain",
            "current_phase": "assigned",
            "previous_phase": None,
            "repo_path": str(tmp_path),
            "branch": "feat/point-fix",
            "status_line": "captain self-registered from direct dispatch",
        },
    )
    assert registration_response.status_code == 201

    store = StateStore(tmp_path)
    snapshot = store.snapshot()
    worker_details = {worker.worker_id: store.get_worker(worker.worker_id) for worker in snapshot.workers}
    supervision = build_supervision_view(
        snapshot,
        worker_details,
        store.list_commands(),
        requested_worker_id=None,
        requested_mission_id=None,
        search_query="",
        current_view="missions",
        saved_view="all-active",
    )

    assert len(supervision["mission_rows"]) == 1
    mission = supervision["mission_rows"][0]
    assert mission["owner"] == "captain-overlord-1"
    assert mission["workers"][0]["worker_id"] == "captain-overlord-1"


def test_worker_event_accepts_process_identity_metadata(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    register_general(client, tmp_path)
    register_captain = client.post(
        "/api/members/register",
        json={
            "member_id": "captain-123",
            "member_token": "captain-secret-token",
            "role": "captain",
            "parent_member_id": "general-123",
            "parent_token": "general-secret-token",
            "repo_path": str(tmp_path),
            "phase": "assigned",
            "status_line": "captain registered for worker intake",
        },
    )
    assert register_captain.status_code == 201

    response = client.post(
        "/api/workers/events",
        json={
            "worker_id": "worker-proc-1",
            "worker_token": "secret-worker-token",
            "role": "worker",
            "parent_worker_id": "captain-123",
            "current_phase": "assigned",
            "previous_phase": None,
            "repo_path": str(tmp_path),
            "host_id": "localhost",
            "process_id": 32123,
            "process_started_at": "2026-03-15T18:05:00Z",
            "status_line": "checking in with pid",
        },
    )

    assert response.status_code == 201
    worker = response.json()["worker"]
    assert worker["process_id"] == 32123
    assert worker["host_id"] == "localhost"
    assert worker["parent_worker_id"] == "captain-123"


def test_terminal_worker_stays_done_without_live_pid(tmp_path: Path) -> None:
    client = build_client(tmp_path)

    assigned = client.post(
        "/api/workers/events",
        json={
            "worker_id": "worker-terminal-1",
            "worker_token": "secret-worker-token",
            "current_phase": "assigned",
            "previous_phase": None,
            "repo_path": str(tmp_path),
            "status_line": "starting terminal-flow check",
        },
    )
    assert assigned.status_code == 201

    terminal = client.post(
        "/api/workers/events",
        json={
            "worker_id": "worker-terminal-1",
            "worker_token": "secret-worker-token",
            "current_phase": "terminal",
            "previous_phase": "assigned",
            "repo_path": str(tmp_path),
            "status_line": "finished successfully",
            "note": "terminal success should stay done",
        },
    )
    assert terminal.status_code == 201
    assert terminal.json()["worker"]["effective_state"] == "done"

    detail = client.get("/api/workers/worker-terminal-1")
    assert detail.status_code == 200
    assert detail.json()["worker"]["effective_state"] == "done"
    assert detail.json()["worker"]["last_heartbeat"]["observed_state"] == "untracked"


def test_pidless_active_worker_is_untracked_not_lost(tmp_path: Path) -> None:
    client = build_client(tmp_path)

    response = client.post(
        "/api/workers/events",
        json={
            "worker_id": "worker-untracked-1",
            "worker_token": "secret-worker-token",
            "current_phase": "assigned",
            "previous_phase": None,
            "repo_path": str(tmp_path),
            "status_line": "working without pid metadata yet",
        },
    )

    assert response.status_code == 201
    assert response.json()["worker"]["effective_state"] == "active"

    detail = client.get("/api/workers/worker-untracked-1")
    assert detail.status_code == 200
    assert detail.json()["worker"]["effective_state"] == "active"
    assert detail.json()["worker"]["last_heartbeat"]["observed_state"] == "untracked"


def test_dashboard_dispatch_form_launches_general_and_shows_recent_order(tmp_path: Path) -> None:
    dispatcher = FakeDispatcher()
    client = build_client(tmp_path, dispatcher=dispatcher)

    response = client.post(
        "/dispatch",
        data={
            "general_worker_id": "general-ui-1",
            "repo_path": str(tmp_path),
            "branch_hint": "feat/operator-dashboard-ui",
            "operator_instruction": "Drive the repo to localhost MVP and leave a summary.",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    follow_up = client.get(response.headers["location"])
    assert follow_up.status_code == 200
    assert "general-ui-1" in follow_up.text
    assert "Drive the repo to localhost MVP and leave a summary." in follow_up.text
    assert str(tmp_path / "dispatch.log") in follow_up.text


def test_worker_status_cli_builds_event_payload_with_abs_repo_path(tmp_path: Path) -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "--worker-id",
            "worker-123",
            "--worker-token",
            "secret-worker-token",
            "--current-phase",
            "validating",
            "--previous-phase",
            "implementing",
            "--repo-path",
            str(tmp_path),
            "--note",
            "running tests",
            "--next-step",
            "open stacked pr",
        ]
    )

    payload = build_payload(args)

    assert payload["worker_id"] == "worker-123"
    assert payload["current_phase"] == "validating"
    assert payload["previous_phase"] == "implementing"
    assert payload["repo_path"] == str(tmp_path.resolve())
    assert payload["note"] == "running tests"


def test_worker_status_cli_uses_env_token(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OVERLORD_WORKER_TOKEN", "env-secret-token")
    parser = build_parser()

    args = parser.parse_args(
        [
            "--worker-id",
            "worker-789",
            "--current-phase",
            "assigned",
            "--repo-path",
            str(tmp_path),
        ]
    )

    payload = build_payload(args)

    assert payload["worker_token"] == "env-secret-token"


def test_worker_status_cli_builds_registration_payload_with_abs_repo_path(tmp_path: Path) -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "register-member",
            "--member-id",
            "captain-123",
            "--member-token",
            "captain-secret-token",
            "--role",
            "captain",
            "--parent-member-id",
            "general-123",
            "--repo-path",
            str(tmp_path),
            "--status-line",
            "registered with overlord",
            "--process-id",
            "12345",
        ]
    )

    payload = build_registration_payload(args)

    assert payload["member_id"] == "captain-123"
    assert payload["parent_member_id"] == "general-123"
    assert payload["repo_path"] == str(tmp_path.resolve())
    assert payload["process_id"] == 12345


def test_worker_status_cli_builds_parent_report_payload() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "parent-report",
            "--member-id",
            "worker-123",
            "--reporter-member-id",
            "captain-123",
            "--reporter-token",
            "captain-secret-token",
            "--observed-status-line",
            "replacement worker spawned",
            "--event-type",
            "replaced_underling",
            "--related-member-id",
            "worker-456",
            "--observed-state",
            "active",
        ]
    )

    payload = build_parent_report_payload(args)

    assert payload["subject_member_id"] == "worker-123"
    assert payload["reporter_member_id"] == "captain-123"
    assert payload["event_type"] == "replaced_underling"
    assert payload["related_member_id"] == "worker-456"
    assert payload["observed_state"] == "active"


def test_worker_status_cli_builds_member_message_payload() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "post-message",
            "--member-id",
            "worker-123",
            "--sender-member-id",
            "captain-123",
            "--sender-token",
            "captain-secret-token",
            "--message-type",
            "progress",
            "--body",
            "still working on the task",
            "--related-member-id",
            "artifact-123",
        ]
    )

    payload = build_member_message_payload(args)

    assert payload["member_id"] == "worker-123"
    assert payload["sender_member_id"] == "captain-123"
    assert payload["message_type"] == "progress"
    assert payload["body"] == "still working on the task"
    assert payload["related_member_id"] == "artifact-123"


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _get_json(url: str) -> tuple[int, dict[str, object]]:
    with request.urlopen(url, timeout=5) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


def _post_json(url: str, payload: dict[str, object]) -> tuple[int, dict[str, object]]:
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=5) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


def _wait_for_server(base_url: str, timeout_seconds: float = 10.0) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            status_code, payload = _get_json(f"{base_url}/healthz")
            if status_code == 200 and payload.get("status") == "ok":
                return
        except Exception as exc:  # pragma: no cover - best effort polling
            last_error = exc
        time.sleep(0.1)
    raise AssertionError(f"server did not become healthy: {last_error}")


def test_live_server_smoke_covers_dispatch_registration_and_graph(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    data_dir = tmp_path / "data"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    codex_path = bin_dir / "codex"
    codex_path.write_text("#!/usr/bin/env bash\ncat >/dev/null\necho '{\"ok\":true}'\n", encoding="utf-8")
    codex_path.chmod(0o755)

    port = _find_free_port()
    base_url = f"http://127.0.0.1:{port}"
    env = os.environ.copy()
    env["OVERLORD_DATA_DIR"] = str(data_dir)
    env["OVERLORD_ALLOWED_REPO_ROOTS"] = str(repo_root)
    env["PATH"] = f"{bin_dir}:{env['PATH']}"

    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "overlord.app:create_app",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(Path(__file__).resolve().parents[1]),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        _wait_for_server(base_url)

        command_status, command_payload = _post_json(
            f"{base_url}/api/commands",
            {
                "general_worker_id": "general-e2e-1",
                "repo_path": str(repo_root),
                "branch_hint": "feat/e2e",
                "operator_instruction": "Run the e2e flow and register your captain.",
            },
        )
        assert command_status == 201
        assert command_payload["command"]["general_worker_id"] == "general-e2e-1"

        register_general_status = subprocess.run(
            [
                sys.executable,
                "-m",
                "overlord.worker_status",
                "--control-plane-url",
                base_url,
                "--worker-id",
                "general-e2e-1",
                "--worker-token",
                "general-e2e-token",
                "--role",
                "general",
                "--current-phase",
                "assigned",
                "--repo-path",
                str(repo_root),
                "--status-line",
                "general online for e2e",
            ],
            cwd=str(Path(__file__).resolve().parents[1]),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert register_general_status.returncode == 0, register_general_status.stderr

        register_captain_status = subprocess.run(
            [
                sys.executable,
                "-m",
                "overlord.worker_status",
                "--control-plane-url",
                base_url,
                "register-member",
                "--member-id",
                "captain-e2e-1",
                "--member-token",
                "captain-e2e-token",
                "--role",
                "captain",
                "--parent-member-id",
                "general-e2e-1",
                "--parent-token",
                "general-e2e-token",
                "--repo-path",
                str(repo_root),
                "--status-line",
                "captain registered through helper",
                "--process-id",
                "12345",
            ],
            cwd=str(Path(__file__).resolve().parents[1]),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert register_captain_status.returncode == 0, register_captain_status.stderr

        parent_report_status = subprocess.run(
            [
                sys.executable,
                "-m",
                "overlord.worker_status",
                "--control-plane-url",
                base_url,
                "parent-report",
                "--member-id",
                "captain-e2e-1",
                "--reporter-member-id",
                "general-e2e-1",
                "--reporter-token",
                "general-e2e-token",
                "--observed-status-line",
                "captain accepted and active",
                "--observed-state",
                "active",
            ],
            cwd=str(Path(__file__).resolve().parents[1]),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert parent_report_status.returncode == 0, parent_report_status.stderr

        graph_status, graph_payload = _get_json(f"{base_url}/api/graph")
        assert graph_status == 200
        graph = graph_payload["graph"]
        nodes = {node["id"]: node for node in graph["nodes"]}
        edges = {(edge["source"], edge["target"]) for edge in graph["edges"]}
        assert "general:general-e2e-1" in nodes
        assert "worker:captain-e2e-1" in nodes
        assert ("overlord", "general:general-e2e-1") in edges
        assert ("general:general-e2e-1", "worker:captain-e2e-1") in edges
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
