from fastapi.testclient import TestClient

from overlord.app import create_app
from overlord.config import Settings


def build_client() -> TestClient:
    settings = Settings(
        OVERLORD_APP_NAME="Overlord Test",
        OVERLORD_DEFAULT_ENVIRONMENT="test",
        OVERLORD_DEFAULT_WORKSPACE="sandbox",
    )
    return TestClient(create_app(settings))


def test_healthz() -> None:
    client = build_client()
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_meta_endpoint_exposes_scaffold_defaults() -> None:
    client = build_client()
    response = client.get("/api/meta")

    assert response.status_code == 200
    assert response.json()["mode"] == "scaffold"
    assert response.json()["defaults"]["environment"] == "test"
    assert response.json()["defaults"]["workspace"] == "sandbox"


def test_homepage_renders_placeholder_ui() -> None:
    client = build_client()
    response = client.get("/")

    assert response.status_code == 200
    assert "Scaffold ready" in response.text
    assert "Overlord Test" in response.text

