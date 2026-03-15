from pathlib import Path

from fastapi.testclient import TestClient

from overlord.app import create_app
from overlord.config import Settings
from overlord.models import DispatchStatus, OperatorCommandCreate, OperatorCommandLaunch
from overlord.worker_status import build_parser, build_payload


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
    assert response.json() == {"status": "ok"}


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
    assert "Localhost control plane" in response.text
    assert "worker-123" in response.text
    assert "Control Pane" in response.text
    assert "Self Report Intake" in response.text
    assert "General Dispatch" in response.text
    assert "Phase Trail" in response.text
    assert "Phase Notes" in response.text
    assert "keeping api and persistence untouched" in response.text


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
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "Accepted." in response.text
    assert "worker-ui-1" in response.text
    assert "manual worker report path works" in response.text


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
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "Rejected." in response.text
    assert "blocked transitions must include a blocker" in response.text


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
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "Launched." in response.text
    assert "general-ui-1" in response.text
    assert "Drive the repo to localhost MVP and leave a summary." in response.text
    assert str(tmp_path / "dispatch.log") in response.text


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
