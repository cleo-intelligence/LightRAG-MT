"""
Tests for API key protection of the operational endpoints.

/ready, /drain (GET and POST), /drain/status, /drain/request, /drain/cancel,
/instances, /metrics and /metrics/all used to answer without any
authentication. They now use the same combined auth dependency as the other
routes. /documents/reclaim already did and is covered as a non-regression.

The app is the real one from create_app(), on offline storages, with the
production auth settings: LIGHTRAG_API_KEY set, WHITELIST_PATHS=/health, with
and without AUTH_ACCOUNTS.

- no key, a wrong key or a guest token: 401/403 and the endpoint never runs;
- valid key: the endpoint answers as before.
"""

import copy
import os
import re
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from starlette.datastructures import URL
from starlette.requests import Request

# lightrag.api.config parses sys.argv on first use: keep pytest's options out
with patch.object(sys, "argv", ["lightrag-server"]):
    from lightrag.api import lightrag_server, utils_api
    from lightrag.api.auth import auth_handler
    from lightrag.api.config import parse_args
    from lightrag.api.workspace_manager import get_rag_for_admin
    from lightrag.instance_registry import get_instance_registry, set_instance_registry
    from lightrag.kg.shared_storage import get_drain_mode, set_drain_mode

    _BASE_ARGS = parse_args()

pytestmark = pytest.mark.offline

API_KEY = "ops-test-key"
WITH_KEY = {"X-API-Key": API_KEY}
ACCOUNTS = {"ops-admin": "ops-password"}

# (method, path, request kwargs) of every protected operational endpoint
OPS_ROUTES = [
    ("GET", "/ready", {}),
    ("GET", "/drain", {}),
    ("POST", "/drain", {"params": {"enable": "true", "reason": "test"}}),
    ("GET", "/drain/status", {}),
    ("POST", "/drain/request", {"params": {"count": 1, "reason": "test"}}),
    ("POST", "/drain/cancel", {}),
    ("GET", "/instances", {}),
    ("GET", "/metrics", {}),
    ("GET", "/metrics/all", {}),
    ("POST", "/documents/reclaim", {"json": {"source_status": "processing"}}),
]
OPS_ROUTE_IDS = [f"{method} {path}" for method, path, _ in OPS_ROUTES]

# Endpoints that answer without credentials on purpose
PUBLIC_ROUTES = {
    ("GET", "/"),
    ("GET", "/docs"),
    ("GET", "/docs/oauth2-redirect"),
    ("GET", "/auth-status"),
    ("POST", "/login"),
    ("GET", "/health"),  # WHITELIST_PATHS
    ("GET", "/webui"),
    ("GET", "/webui/"),
}

HTTP_METHODS = {"get", "put", "post", "delete", "options", "head", "patch", "trace"}


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def app(tmp_path_factory):
    """The real server app on offline storages, protected by API_KEY."""
    tmp = tmp_path_factory.mktemp("ops-routes-auth")
    args = copy.copy(_BASE_ARGS)
    args.working_dir = str(tmp / "rag")
    args.input_dir = str(tmp / "inputs")
    args.kv_storage = "JsonKVStorage"
    args.doc_status_storage = "JsonDocStatusStorage"
    args.graph_storage = "NetworkXStorage"
    args.vector_storage = "NanoVectorDBStorage"
    args.llm_binding = "openai"
    args.embedding_binding = "openai"
    args.workspace = ""
    args.default_workspace = ""
    args.ssl = False
    args.key = API_KEY
    # create_app() prefers the env var over args.key: pin it to the test key
    with patch.dict(os.environ, {"LIGHTRAG_API_KEY": API_KEY}):
        return lightrag_server.create_app(args)


def _use_production_auth(monkeypatch, accounts):
    """WHITELIST_PATHS=/health, with or without AUTH_ACCOUNTS."""
    monkeypatch.setattr(utils_api, "whitelist_patterns", [("/health", False)])
    monkeypatch.setattr(utils_api, "auth_configured", bool(accounts))
    monkeypatch.setattr(auth_handler, "accounts", accounts)


@pytest.fixture(params=[{}, ACCOUNTS], ids=["key-only", "key+accounts"])
def client(request, app, monkeypatch):
    _use_production_auth(monkeypatch, dict(request.param))
    return TestClient(app)


@pytest.fixture
def accounts_client(app, monkeypatch):
    _use_production_auth(monkeypatch, dict(ACCOUNTS))
    return TestClient(app)


@pytest.fixture
def key_only_client(app, monkeypatch):
    _use_production_auth(monkeypatch, {})
    return TestClient(app)


@pytest.fixture(autouse=True)
def ops_backend(app, monkeypatch):
    """Drain mode off, a fake instance registry and a fake admin RAG."""
    registry = MagicMock()
    registry.instance_id = "instance-a"
    registry.hostname = "host-a"
    registry.get_all_instances = AsyncMock(
        return_value=[{"instance_id": "instance-a", "alive": True}]
    )
    registry.get_drain_status = AsyncMock(
        return_value={"total_instances": 2, "safe_to_scale_down": False}
    )
    registry.request_drain = AsyncMock(return_value=["instance-b"])
    registry.cancel_drain = AsyncMock(return_value=1)
    registry.get_all_instances_with_metrics = AsyncMock(
        return_value=[
            {
                "instance_id": "instance-a",
                "hostname": "host-a",
                "metrics": {
                    "llm_active_calls": 1,
                    "processing_count": 2,
                    "db_pool_active": 3,
                    "pipelines_busy": 0,
                },
            }
        ]
    )

    rag = MagicMock()
    rag.doc_status.reclaim_stale_processing = AsyncMock(
        return_value={"status": "ok", "reclaimed_count": 1, "reclaimed_ids": ["d1"]}
    )
    admin_rag = MagicMock(return_value=rag)

    pipeline_busy = MagicMock(return_value={"busy": False, "busy_workspaces": []})
    monkeypatch.setattr(lightrag_server, "is_any_pipeline_busy", pipeline_busy)

    previous_registry = get_instance_registry()
    set_drain_mode(enabled=False)
    set_instance_registry(registry)
    app.dependency_overrides[get_rag_for_admin] = lambda: admin_rag()
    yield SimpleNamespace(
        registry=registry, rag=rag, admin_rag=admin_rag, pipeline_busy=pipeline_busy
    )
    app.dependency_overrides.pop(get_rag_for_admin, None)
    set_instance_registry(previous_registry)
    set_drain_mode(enabled=False)


def assert_endpoint_not_run(backend):
    """The endpoint body never ran: no drain change, no registry/storage call."""
    assert get_drain_mode()["enabled"] is False
    backend.pipeline_busy.assert_not_called()
    for name in (
        "get_all_instances",
        "get_drain_status",
        "request_drain",
        "cancel_drain",
        "get_all_instances_with_metrics",
    ):
        getattr(backend.registry, name).assert_not_awaited()
    backend.admin_rag.assert_not_called()
    backend.rag.doc_status.reclaim_stale_processing.assert_not_awaited()


def _api_routes(app):
    """(method, path) of every API route, whatever the FastAPI version."""
    # Recent FastAPI versions no longer copy included routers into app.routes
    routes = {
        (method.upper(), path)
        for path, operations in app.openapi()["paths"].items()
        for method in operations
        if method in HTTP_METHODS
    }
    routes |= {
        (method, route.path)
        for route in app.routes
        if isinstance(route, APIRoute)
        for method in route.methods
    }
    return routes


# =============================================================================
# Without valid credentials: refused, endpoint never runs
# =============================================================================


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"X-API-Key": "wrong-key"},
        {"X-API-Key": API_KEY[:-1]},
        {"X-API-Key": API_KEY + "x"},
        {"X-API-Key": API_KEY.upper()},
    ],
    ids=["no-key", "wrong-key", "key-prefix", "key-suffix", "key-upper"],
)
@pytest.mark.parametrize("method,path,kwargs", OPS_ROUTES, ids=OPS_ROUTE_IDS)
def test_ops_route_refused_without_valid_key(
    client, ops_backend, method, path, kwargs, headers
):
    response = client.request(method, path, headers=headers, **kwargs)

    assert response.status_code in (401, 403)
    assert_endpoint_not_run(ops_backend)


@pytest.mark.parametrize("method,path,kwargs", OPS_ROUTES, ids=OPS_ROUTE_IDS)
def test_ops_route_refuses_guest_token(client, ops_backend, method, path, kwargs):
    guest_token = auth_handler.create_token(username="guest", role="guest")

    response = client.request(
        method, path, headers={"Authorization": f"Bearer {guest_token}"}, **kwargs
    )

    assert response.status_code in (401, 403)
    assert_endpoint_not_run(ops_backend)


def test_guest_token_from_auth_status_does_not_replace_the_key(
    key_only_client, ops_backend
):
    """Without AUTH_ACCOUNTS, /auth-status hands a guest token to anyone."""
    guest_token = key_only_client.get("/auth-status").json()["access_token"]

    response = key_only_client.post(
        "/drain",
        params={"enable": "true"},
        headers={"Authorization": f"Bearer {guest_token}"},
    )

    assert response.status_code == 403
    assert_endpoint_not_run(ops_backend)


def test_guest_token_alongside_the_key_is_ignored(key_only_client):
    """With an API key configured, the key decides, not the guest token."""
    guest_token = auth_handler.create_token(username="guest", role="guest")

    response = key_only_client.get(
        "/drain", headers={"Authorization": f"Bearer {guest_token}", **WITH_KEY}
    )

    assert response.status_code == 200


def test_no_other_route_answers_without_key(app, client):
    """Every API route outside PUBLIC_ROUTES refuses an anonymous request."""
    checked = sorted(_api_routes(app) - PUBLIC_ROUTES)
    # A route listing that misses the included routers must fail, not pass
    assert ("POST", "/documents/reclaim") in checked
    assert ("POST", "/query") in checked
    assert len(checked) >= 40

    for method, route_path in checked:
        response = client.request(method, re.sub(r"{[^}]+}", "x", route_path))
        assert response.status_code in (401, 403), f"{method} {route_path}"


def test_health_stays_public(client, monkeypatch):
    """Render's health check calls /health without a key."""
    # Pipeline status is set up by the lifespan, which these tests don't run
    monkeypatch.setattr(
        lightrag_server, "get_namespace_data", AsyncMock(return_value={})
    )
    monkeypatch.setattr(lightrag_server, "cleanup_keyed_lock", MagicMock())

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


# =============================================================================
# With the key: unchanged behaviour
# =============================================================================


def test_ready_with_key(client):
    response = client.get("/ready", headers=WITH_KEY)
    assert response.status_code == 200
    assert response.json() == {
        "ready": True,
        "drain_mode": False,
        "busy_workspaces": [],
    }

    set_drain_mode(enabled=True, reason="maintenance")
    response = client.get("/ready", headers=WITH_KEY)
    assert response.status_code == 503
    assert response.json()["drain_reason"] == "maintenance"


def test_get_drain_with_key(client, ops_backend):
    set_drain_mode(enabled=True, reason="maintenance")

    response = client.get("/drain", headers=WITH_KEY)

    assert response.status_code == 200
    body = response.json()
    assert body["drain_mode"] is True
    assert body["reason"] == "maintenance"
    assert body["pipelines_busy"] is False
    assert body["busy_workspaces"] == []


def test_post_drain_with_key_toggles_drain_mode(client):
    response = client.post(
        "/drain", params={"enable": "true", "reason": "scale-down"}, headers=WITH_KEY
    )
    assert response.status_code == 200
    assert response.json()["drain_mode"] is True
    assert get_drain_mode() == {
        "enabled": True,
        "reason": "scale-down",
        "started_at": response.json()["started_at"],
    }

    response = client.post("/drain", params={"enable": "false"}, headers=WITH_KEY)
    assert response.status_code == 200
    assert response.json()["drain_mode"] is False
    assert get_drain_mode()["enabled"] is False


def test_drain_status_with_key(client, ops_backend):
    response = client.get("/drain/status", headers=WITH_KEY)

    assert response.status_code == 200
    assert response.json() == {
        "total_instances": 2,
        "safe_to_scale_down": False,
        "enabled": True,
        "this_instance": "instance-a",
    }


def test_drain_request_with_key(client, ops_backend):
    response = client.post(
        "/drain/request", params={"count": 2, "reason": "scale-down"}, headers=WITH_KEY
    )

    assert response.status_code == 200
    assert response.json() == {
        "success": True,
        "drained_count": 1,
        "drained_instances": ["instance-b"],
        "reason": "scale-down",
    }
    ops_backend.registry.request_drain.assert_awaited_once_with(
        count=2, reason="scale-down"
    )


def test_drain_cancel_with_key(client, ops_backend):
    response = client.post("/drain/cancel", headers=WITH_KEY)

    assert response.status_code == 200
    assert response.json() == {"success": True, "cancelled_count": 1}
    ops_backend.registry.cancel_drain.assert_awaited_once_with()


def test_instances_with_key(client, ops_backend):
    response = client.get("/instances", headers=WITH_KEY)

    assert response.status_code == 200
    assert response.json() == {
        "enabled": True,
        "this_instance": "instance-a",
        "instances": [{"instance_id": "instance-a", "alive": True}],
    }


def test_metrics_with_key(client):
    response = client.get("/metrics", headers=WITH_KEY)

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert 'lightrag_queue_depth{workspace="all"}' in response.text
    assert 'lightrag_drain_mode{instance_id="instance-a"} 0' in response.text


def test_metrics_all_with_key(client):
    response = client.get("/metrics/all", headers=WITH_KEY)

    assert response.status_code == 200
    body = response.json()
    assert body["instance_count"] == 1
    assert body["this_instance"] == "instance-a"
    assert body["registry_enabled"] is True
    assert body["aggregated"] == {
        "total_llm_active_calls": 1,
        "total_processing_count": 2,
        "total_db_pool_active": 3,
        "total_pipelines_busy": 0,
    }


def test_reclaim_with_key(client, ops_backend):
    response = client.post(
        "/documents/reclaim",
        json={"source_status": "processing", "stale_minutes": 45},
        headers=WITH_KEY,
    )

    assert response.status_code == 200
    assert response.json()["reclaimed_ids"] == ["d1"]
    ops_backend.rag.doc_status.reclaim_stale_processing.assert_awaited_once_with(
        stale_minutes=45, target_status="pending", workspace=None
    )


def test_login_token_works_like_the_key(accounts_client):
    """Same control as the other routes: an AUTH_ACCOUNTS login is accepted."""
    login = accounts_client.post(
        "/login", data={"username": "ops-admin", "password": "ops-password"}
    )
    assert login.status_code == 200
    token = login.json()["access_token"]

    response = accounts_client.get(
        "/drain", headers={"Authorization": f"Bearer {token}"}
    )

    assert response.status_code == 200
    assert response.json()["drain_mode"] is False


# =============================================================================
# Whitelist matching: only the routed path counts
# =============================================================================


def test_whitelist_ignores_request_url(client, ops_backend, monkeypatch):
    """Whatever request.url says, a request to /drain is not a /health one."""
    monkeypatch.setattr(
        Request, "url", property(lambda self: URL("http://testserver/health"))
    )

    response = client.post("/drain", params={"enable": "true"})

    assert response.status_code in (401, 403)
    assert_endpoint_not_run(ops_backend)


@pytest.mark.parametrize("host", ["testserver/health?", "testserver/health#"])
def test_host_header_cannot_fake_a_public_path(client, ops_backend, host):
    """Starlette < 1.7 rebuilds request.url from the raw Host header."""
    response = client.post("/drain", params={"enable": "true"}, headers={"host": host})

    assert response.status_code in (401, 403)
    assert_endpoint_not_run(ops_backend)
