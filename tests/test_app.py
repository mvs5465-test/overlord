from pathlib import Path

from fastapi.testclient import TestClient

from overlord.app import create_app
from overlord.config import Settings
from overlord.worker_status import build_parser, build_payload


def build_client(tmp_path: Path) -> TestClient:
    settings = Settings(
        OVERLORD_APP_NAME="Overlord Test",
        OVERLORD_DEFAULT_ENVIRONMENT="test",
        OVERLORD_DEFAULT_WORKSPACE="sandbox",
        OVERLORD_DATA_DIR=tmp_path,
        OVERLORD_ALLOWED_REPO_ROOTS=str(tmp_path),
    )
    return TestClient(create_app(settings))


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


def test_homepage_renders_live_dashboard(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    register_worker(client, tmp_path)

    response = client.get("/")

    assert response.status_code == 200
    assert "Localhost control plane" in response.text
    assert "worker-123" in response.text
    assert "claiming backend slice" in response.text


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
