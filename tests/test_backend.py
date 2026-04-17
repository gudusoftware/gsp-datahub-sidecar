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
        "self_hosted": {"url": "https://docs.gudusoft.com/docker/"},
    },
}


def _body(call) -> str:
    b = call.request.body
    return b.decode() if isinstance(b, bytes) else (b or "")


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
    assert "docs.gudusoft.com/docker/" in str(exc_info.value)


AUTHENTICATED_LINEAGE_URL = (
    "https://api.gudusoft.com/gspLive_backend/sqlflow/generation/sqlflow/exportFullLineageAsJson"
)
AUTHENTICATED_TOKEN_URL = "https://api.gudusoft.com/gspLive_backend/user/generateToken"


@responses.activate
def test_authenticated_token_flow():
    """Authenticated (cloud) mode uses the same token-exchange flow as self-hosted."""
    responses.add(
        responses.POST,
        AUTHENTICATED_TOKEN_URL,
        json={"code": "200", "userId": "cloud-user", "token": "jwt-cloud"},
        status=200,
    )
    responses.add(responses.POST, AUTHENTICATED_LINEAGE_URL, json=MOCK_SUCCESS, status=200)

    backend = AuthenticatedBackend(url=AUTHENTICATED_LINEAGE_URL, user_id="cloud-user", secret_key="sk-test")
    result = backend.get_lineage("SELECT 1", "dbvbigquery")
    assert result["code"] == 200

    # Token exchange happens against api.gudusoft.com (no /api/ prefix).
    assert responses.calls[0].request.url == AUTHENTICATED_TOKEN_URL
    token_body = _body(responses.calls[0])
    assert "userId=cloud-user" in token_body
    assert "secretKey=sk-test" in token_body

    # Lineage call sends userId + token, not a Bearer header.
    lineage_body = _body(responses.calls[1])
    assert "userId=cloud-user" in lineage_body
    assert "token=jwt-cloud" in lineage_body
    assert "secretKey" not in lineage_body
    assert "Authorization" not in responses.calls[1].request.headers


@responses.activate
def test_authenticated_token_generation_failure():
    """Bad credentials surface the server's error message."""
    responses.add(
        responses.POST,
        AUTHENTICATED_TOKEN_URL,
        json={"code": "401", "error": "Invalid user or token, access deny."},
        status=200,
    )

    backend = AuthenticatedBackend(url=AUTHENTICATED_LINEAGE_URL, user_id="cloud-user", secret_key="sk-bad")
    with pytest.raises(SQLFlowError, match="token generation failed"):
        backend.get_lineage("SELECT 1", "dbvbigquery")


SELF_HOSTED_LINEAGE_URL = (
    "http://localhost:8165/api/gspLive_backend/sqlflow/generation/sqlflow/exportFullLineageAsJson"
)
SELF_HOSTED_TOKEN_URL = "http://localhost:8165/api/gspLive_backend/user/generateToken"


@responses.activate
def test_self_hosted_no_credentials():
    """Without credentials, no token fetch and no auth fields are sent."""
    responses.add(responses.POST, SELF_HOSTED_LINEAGE_URL, json=MOCK_SUCCESS, status=200)

    backend = SelfHostedBackend(url=SELF_HOSTED_LINEAGE_URL)
    result = backend.get_lineage("SELECT 1", "dbvbigquery")
    assert result["code"] == 200

    assert len(responses.calls) == 1
    req = responses.calls[0].request
    assert req.headers["Content-Type"].startswith("application/x-www-form-urlencoded")
    body = _body(responses.calls[0])
    assert "sqltext=SELECT+1" in body
    assert "userId" not in body
    assert "token" not in body


@responses.activate
def test_self_hosted_token_flow():
    """With credentials, the backend calls generateToken then uses the returned token."""
    responses.add(
        responses.POST,
        SELF_HOSTED_TOKEN_URL,
        json={"code": "200", "userId": "real-user", "token": "jwt-abc"},
        status=200,
    )
    responses.add(responses.POST, SELF_HOSTED_LINEAGE_URL, json=MOCK_SUCCESS, status=200)

    backend = SelfHostedBackend(url=SELF_HOSTED_LINEAGE_URL, user_id="real-user", secret_key="sk-local")
    result = backend.get_lineage("SELECT 1", "dbvbigquery")
    assert result["code"] == 200

    # 1. Token request uses userId + secretKey.
    token_body = _body(responses.calls[0])
    assert responses.calls[0].request.url.endswith("/user/generateToken")
    assert "userId=real-user" in token_body
    assert "secretKey=sk-local" in token_body

    # 2. Lineage request uses userId + token (not secretKey).
    lineage_body = _body(responses.calls[1])
    assert "userId=real-user" in lineage_body
    assert "token=jwt-abc" in lineage_body
    assert "secretKey" not in lineage_body
    assert "Authorization" not in responses.calls[1].request.headers


@responses.activate
def test_self_hosted_token_is_cached():
    """Multiple get_lineage calls reuse the same token."""
    responses.add(
        responses.POST,
        SELF_HOSTED_TOKEN_URL,
        json={"code": "200", "userId": "real-user", "token": "jwt-abc"},
        status=200,
    )
    responses.add(responses.POST, SELF_HOSTED_LINEAGE_URL, json=MOCK_SUCCESS, status=200)
    responses.add(responses.POST, SELF_HOSTED_LINEAGE_URL, json=MOCK_SUCCESS, status=200)

    backend = SelfHostedBackend(url=SELF_HOSTED_LINEAGE_URL, user_id="real-user", secret_key="sk-local")
    backend.get_lineage("SELECT 1", "dbvbigquery")
    backend.get_lineage("SELECT 2", "dbvbigquery")

    token_calls = [c for c in responses.calls if c.request.url.endswith("/user/generateToken")]
    assert len(token_calls) == 1


@responses.activate
def test_self_hosted_demo_user_skips_token_request():
    """The gudu|0123456789 demo user accepts the literal string 'token'."""
    responses.add(responses.POST, SELF_HOSTED_LINEAGE_URL, json=MOCK_SUCCESS, status=200)

    backend = SelfHostedBackend(url=SELF_HOSTED_LINEAGE_URL, user_id="gudu|0123456789", secret_key="anything")
    result = backend.get_lineage("SELECT 1", "dbvbigquery")
    assert result["code"] == 200

    assert len(responses.calls) == 1  # no generateToken round-trip
    body = _body(responses.calls[0])
    assert "userId=gudu%7C0123456789" in body
    assert "token=token" in body


@responses.activate
def test_self_hosted_token_refresh_on_401():
    """If the cached token is rejected, the backend fetches a new one and retries once."""
    responses.add(
        responses.POST,
        SELF_HOSTED_TOKEN_URL,
        json={"code": "200", "userId": "real-user", "token": "jwt-new"},
        status=200,
    )
    responses.add(
        responses.POST,
        SELF_HOSTED_LINEAGE_URL,
        json={"code": 401, "error": "Invalid user or token, access deny."},
        status=200,
    )
    responses.add(responses.POST, SELF_HOSTED_LINEAGE_URL, json=MOCK_SUCCESS, status=200)

    backend = SelfHostedBackend(url=SELF_HOSTED_LINEAGE_URL, user_id="real-user", secret_key="sk-local")
    backend._token = "jwt-old"  # simulate cached stale token; skip initial generateToken call
    result = backend.get_lineage("SELECT 1", "dbvbigquery")
    assert result["code"] == 200

    lineage_calls = [c for c in responses.calls if c.request.url.endswith("/exportFullLineageAsJson")]
    assert len(lineage_calls) == 2
    assert "token=jwt-old" in _body(lineage_calls[0])
    assert "token=jwt-new" in _body(lineage_calls[1])


@responses.activate
def test_self_hosted_token_generation_failure():
    """A failed generateToken surfaces a clear error."""
    responses.add(
        responses.POST,
        SELF_HOSTED_TOKEN_URL,
        json={"code": "401", "error": "Invalid user or token, access deny."},
        status=200,
    )

    backend = SelfHostedBackend(url=SELF_HOSTED_LINEAGE_URL, user_id="real-user", secret_key="wrong")
    with pytest.raises(SQLFlowError, match="token generation failed"):
        backend.get_lineage("SELECT 1", "dbvbigquery")


def test_create_backend_anonymous():
    cfg = SQLFlowConfig(mode="anonymous")
    backend = create_backend(cfg)
    assert isinstance(backend, AnonymousBackend)


def test_create_backend_authenticated():
    cfg = SQLFlowConfig(mode="authenticated", user_id="cloud-user", secret_key="sk-test")
    backend = create_backend(cfg)
    assert isinstance(backend, AuthenticatedBackend)
    assert backend.user_id == "cloud-user"
    assert backend.secret_key == "sk-test"


def test_create_backend_self_hosted():
    cfg = SQLFlowConfig(mode="self_hosted")
    backend = create_backend(cfg)
    assert isinstance(backend, SelfHostedBackend)
