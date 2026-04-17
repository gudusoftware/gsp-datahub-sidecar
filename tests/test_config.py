"""Tests for configuration loading."""

import os
import tempfile

import pytest

from gsp_datahub_sidecar.config import DEFAULT_URLS, load_config


def test_defaults():
    cfg = load_config(config_path=None)
    assert cfg.sqlflow.mode == "anonymous"
    assert cfg.sqlflow.effective_url == DEFAULT_URLS["anonymous"]
    assert cfg.sqlflow.db_vendor == "dbvbigquery"
    assert cfg.datahub.server == "http://localhost:8080"


def test_yaml_loading(monkeypatch):
    # Clear env vars that would override YAML
    monkeypatch.delenv("GSP_SQLFLOW_SECRET_KEY", raising=False)
    monkeypatch.delenv("GSP_DATAHUB_TOKEN", raising=False)

    yaml_content = """
sqlflow:
  mode: authenticated
  secret_key: sk-test123
  db_vendor: dbvoracle
datahub:
  server: http://gms:8080
  token: test-token
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f.flush()
        cfg = load_config(f.name)

    assert cfg.sqlflow.mode == "authenticated"
    assert cfg.sqlflow.secret_key == "sk-test123"
    assert cfg.sqlflow.db_vendor == "dbvoracle"
    assert cfg.datahub.server == "http://gms:8080"
    assert cfg.datahub.token == "test-token"
    os.unlink(f.name)


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("GSP_BACKEND_MODE", "self_hosted")
    monkeypatch.setenv("GSP_SQLFLOW_URL", "http://myhost:8081/api")
    monkeypatch.setenv("GSP_DATAHUB_SERVER", "http://datahub:8080")

    cfg = load_config(config_path=None)
    assert cfg.sqlflow.mode == "self_hosted"
    assert cfg.sqlflow.url == "http://myhost:8081/api"
    assert cfg.sqlflow.effective_url == "http://myhost:8081/api"
    assert cfg.datahub.server == "http://datahub:8080"


def test_env_overrides_yaml(monkeypatch):
    yaml_content = """
sqlflow:
  mode: anonymous
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f.flush()

        # Env should override YAML
        monkeypatch.setenv("GSP_BACKEND_MODE", "self_hosted")
        cfg = load_config(f.name)

    assert cfg.sqlflow.mode == "self_hosted"
    os.unlink(f.name)


def test_authenticated_requires_secret_key(monkeypatch):
    monkeypatch.delenv("GSP_SQLFLOW_SECRET_KEY", raising=False)

    yaml_content = """
sqlflow:
  mode: authenticated
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f.flush()
        with pytest.raises(ValueError, match="secret_key is required"):
            load_config(f.name)
    os.unlink(f.name)


def test_invalid_mode():
    yaml_content = """
sqlflow:
  mode: invalid_mode
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f.flush()
        with pytest.raises(ValueError, match="Invalid sqlflow.mode"):
            load_config(f.name)
    os.unlink(f.name)


def test_effective_url_defaults():
    cfg = load_config(config_path=None)
    assert cfg.sqlflow.mode == "anonymous"
    assert "anonymous" in cfg.sqlflow.effective_url

    # Explicit URL should override default
    cfg.sqlflow.url = "http://custom:8080/api"
    assert cfg.sqlflow.effective_url == "http://custom:8080/api"
