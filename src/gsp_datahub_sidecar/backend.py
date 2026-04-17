"""SQLFlow backend — calls the lineage API in anonymous, authenticated, or self-hosted mode."""

import logging
from abc import ABC, abstractmethod
from typing import Any

import requests

from .config import SQLFlowConfig

logger = logging.getLogger(__name__)


class SQLFlowBackend(ABC):
    """Base class for SQLFlow API backends."""

    @abstractmethod
    def get_lineage(self, sql: str, db_vendor: str, **kwargs) -> dict[str, Any]:
        """Send SQL to SQLFlow and return the parsed lineage JSON.

        Returns the full response dict (with 'code' and 'data' keys).
        Raises SQLFlowError on failure.
        """

    def _build_payload(self, sql: str, db_vendor: str, **kwargs) -> dict:
        return {
            "sqltext": sql,
            "dbvendor": db_vendor,
            "showRelationType": kwargs.get("show_relation_type", "fdd"),
        }


class SQLFlowError(Exception):
    """Raised when the SQLFlow API returns an error."""

    def __init__(self, message: str, status_code: int = 0, response_body: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body or {}


class RateLimitError(SQLFlowError):
    """Raised when the anonymous tier rate limit is exceeded (HTTP 429)."""

    def __init__(self, response_body: dict):
        upgrade = response_body.get("upgrade", {})
        personal_url = upgrade.get("personal_key", {}).get("url", "https://docs.gudusoft.com/sign-up/")
        docker_url = upgrade.get("self_hosted", {}).get("url", "https://sqlflow.gudusoft.com/docker")
        message = (
            f"Anonymous API rate limit exceeded. "
            f"To continue:\n"
            f"  1. Get a free personal key (10k/month): {personal_url}\n"
            f"  2. Deploy self-hosted (unlimited):    {docker_url}\n"
            f"  See sidecar.yaml.example for configuration."
        )
        super().__init__(message, status_code=429, response_body=response_body)


class AnonymousBackend(SQLFlowBackend):
    """Tier 1: No auth. Rate-limited per IP (50/day)."""

    def __init__(self, url: str):
        self.url = url

    def get_lineage(self, sql: str, db_vendor: str, **kwargs) -> dict[str, Any]:
        payload = self._build_payload(sql, db_vendor, **kwargs)
        resp = requests.post(self.url, json=payload, timeout=120)

        if resp.status_code == 429:
            raise RateLimitError(resp.json())
        resp.raise_for_status()
        return resp.json()


class AuthenticatedBackend(SQLFlowBackend):
    """Tier 2: Personal API key (secret key). Higher quota."""

    def __init__(self, url: str, secret_key: str):
        self.url = url
        self.secret_key = secret_key

    def get_lineage(self, sql: str, db_vendor: str, **kwargs) -> dict[str, Any]:
        payload = self._build_payload(sql, db_vendor, **kwargs)
        headers = {"Authorization": f"Bearer {self.secret_key}"}
        resp = requests.post(self.url, json=payload, headers=headers, timeout=120)

        if resp.status_code == 401:
            raise SQLFlowError(
                "Authentication failed. Check your secret_key in sidecar.yaml. "
                "Get a key at https://docs.gudusoft.com/sign-up/",
                status_code=401,
            )
        resp.raise_for_status()
        return resp.json()


class SelfHostedBackend(SQLFlowBackend):
    """Tier 3: Self-hosted SQLFlow Docker. Unlimited, data stays in VPC."""

    def __init__(self, url: str, secret_key: str | None = None):
        self.url = url
        self.secret_key = secret_key

    def get_lineage(self, sql: str, db_vendor: str, **kwargs) -> dict[str, Any]:
        payload = self._build_payload(sql, db_vendor, **kwargs)
        headers = {}
        if self.secret_key:
            headers["Authorization"] = f"Bearer {self.secret_key}"
        resp = requests.post(self.url, json=payload, headers=headers, timeout=120)

        if resp.status_code != 200:
            raise SQLFlowError(
                f"Self-hosted SQLFlow returned HTTP {resp.status_code}. "
                f"Check that SQLFlow Docker is running at {self.url}",
                status_code=resp.status_code,
            )
        return resp.json()


def create_backend(config: SQLFlowConfig) -> SQLFlowBackend:
    """Factory: create the right backend based on config mode."""
    url = config.effective_url

    if config.mode == "anonymous":
        logger.info("Using anonymous backend: %s (50 calls/day per IP)", url)
        return AnonymousBackend(url=url)

    if config.mode == "authenticated":
        logger.info("Using authenticated backend: %s", url)
        return AuthenticatedBackend(url=url, secret_key=config.secret_key)

    if config.mode == "self_hosted":
        logger.info("Using self-hosted backend: %s", url)
        return SelfHostedBackend(url=url, secret_key=config.secret_key)

    raise ValueError(f"Unknown backend mode: {config.mode}")
