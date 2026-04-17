"""Tests for backend — SQLFlow API client."""

import pytest
import responses

from gsp_datahub_sidecar.backend import (
    AnonymousBackend,
    AuthenticatedBackend,
    RateLimitError,
    SQLFlowError,
    SelfHostedBackend,
    create_backend,
)
from gsp_datahub_sidecar.config import SQLFlowConfig


MOCK_SUCCESS = {"code": 200, "data": {"sqlflow": {"relationships": []}}}

MOCK_429 = {
    "code": 429,
    "error": "rate_limit_exceeded",
    "message": "Anonymous API limit reached (50 calls/day).",
    "upgrade": {
        "personal_key": {"url": "https://docs.gudusoft.com/sign-up/"},
        "self_hosted": {"url": "https://sqlflow.gudusoft.com/docker"},
    },
}


@responses.activate
def test_anonymous_success():
    url = "https://api.gudusoft.com/gspLive_backend/api/anonymous/lineage"
    responses.add(responses.POST, url, json=MOCK_SUCCESS, status=200)

    backend = AnonymousBackend(url=url)
    result = backend.get_lineage("SELECT 1", "dbvbigquery")
    assert result["code"] == 200


@responses.activate
def test_anonymous_rate_limit():
    url = "https://api.gudusoft.com/gspLive_backend/api/anonymous/lineage"
    responses.add(responses.POST, url, json=MOCK_429, status=429)

    backend = AnonymousBackend(url=url)
    with pytest.raises(RateLimitError) as exc_info:
        backend.get_lineage("SELECT 1", "dbvbigquery")

    assert "rate limit exceeded" in str(exc_info.value).lower()
    assert "docs.gudusoft.com/sign-up" in str(exc_info.value)
    assert "sqlflow.gudusoft.com/docker" in str(exc_info.value)


@responses.activate
def test_authenticated_success():
    url = "https://api.gudusoft.com/gspLive_backend/v1/sqlflow/sqlflow/exportFullLineageAsJson"
    responses.add(responses.POST, url, json=MOCK_SUCCESS, status=200)

    backend = AuthenticatedBackend(url=url, secret_key="sk-test")
    result = backend.get_lineage("SELECT 1", "dbvbigquery")
    assert result["code"] == 200

    # Verify auth header was sent
    assert responses.calls[0].request.headers["Authorization"] == "Bearer sk-test"


@responses.activate
def test_authenticated_401():
    url = "https://api.gudusoft.com/gspLive_backend/v1/sqlflow/sqlflow/exportFullLineageAsJson"
    responses.add(responses.POST, url, json={"code": 401}, status=401)

    backend = AuthenticatedBackend(url=url, secret_key="sk-bad")
    with pytest.raises(SQLFlowError, match="Authentication failed"):
        backend.get_lineage("SELECT 1", "dbvbigquery")


@responses.activate
def test_self_hosted_success():
    url = "http://localhost:8081/gspLive_backend/v1/sqlflow/sqlflow/exportFullLineageAsJson"
    responses.add(responses.POST, url, json=MOCK_SUCCESS, status=200)

    backend = SelfHostedBackend(url=url)
    result = backend.get_lineage("SELECT 1", "dbvbigquery")
    assert result["code"] == 200

    # No auth header when secret_key is None
    assert "Authorization" not in responses.calls[0].request.headers


@responses.activate
def test_self_hosted_with_key():
    url = "http://localhost:8081/gspLive_backend/v1/sqlflow/sqlflow/exportFullLineageAsJson"
    responses.add(responses.POST, url, json=MOCK_SUCCESS, status=200)

    backend = SelfHostedBackend(url=url, secret_key="sk-local")
    result = backend.get_lineage("SELECT 1", "dbvbigquery")
    assert result["code"] == 200
    assert responses.calls[0].request.headers["Authorization"] == "Bearer sk-local"


def test_create_backend_anonymous():
    cfg = SQLFlowConfig(mode="anonymous")
    backend = create_backend(cfg)
    assert isinstance(backend, AnonymousBackend)


def test_create_backend_authenticated():
    cfg = SQLFlowConfig(mode="authenticated", secret_key="sk-test")
    backend = create_backend(cfg)
    assert isinstance(backend, AuthenticatedBackend)


def test_create_backend_self_hosted():
    cfg = SQLFlowConfig(mode="self_hosted")
    backend = create_backend(cfg)
    assert isinstance(backend, SelfHostedBackend)
