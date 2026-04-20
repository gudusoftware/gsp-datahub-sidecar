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
    monkeypatch.delenv("GSP_SQLFLOW_USER_ID", raising=False)
    monkeypatch.delenv("GSP_SQLFLOW_SECRET_KEY", raising=False)
    monkeypatch.delenv("GSP_DATAHUB_TOKEN", raising=False)

    yaml_content = """
sqlflow:
  mode: authenticated
  user_id: cloud-user-123
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
    assert cfg.sqlflow.user_id == "cloud-user-123"
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


def test_authenticated_requires_user_id_and_secret_key(monkeypatch):
    monkeypatch.delenv("GSP_SQLFLOW_USER_ID", raising=False)
    monkeypatch.delenv("GSP_SQLFLOW_SECRET_KEY", raising=False)

    # Neither set
    yaml_content = """
sqlflow:
  mode: authenticated
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f.flush()
        with pytest.raises(ValueError, match="user_id and sqlflow.secret_key"):
            load_config(f.name)
    os.unlink(f.name)

    # Only secret_key set — user_id still required
    yaml_content = """
sqlflow:
  mode: authenticated
  secret_key: sk-test
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f.flush()
        with pytest.raises(ValueError, match="user_id and sqlflow.secret_key"):
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


def test_local_jar_requires_jar_path(monkeypatch):
    monkeypatch.delenv("GSP_JAR_PATH", raising=False)
    yaml_content = """
sqlflow:
  mode: local_jar
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f.flush()
        with pytest.raises(ValueError, match="jar_path is required"):
            load_config(f.name)
    os.unlink(f.name)


def test_local_jar_loads_jar_path_from_yaml(monkeypatch):
    monkeypatch.delenv("GSP_JAR_PATH", raising=False)
    yaml_content = """
sqlflow:
  mode: local_jar
  jar_path: /opt/gsp/gsqlparser-4.1.0.13-shaded.jar
  java_bin: /opt/jdk21/bin/java
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f.flush()
        cfg = load_config(f.name)

    assert cfg.sqlflow.mode == "local_jar"
    assert cfg.sqlflow.jar_path == "/opt/gsp/gsqlparser-4.1.0.13-shaded.jar"
    assert cfg.sqlflow.java_bin == "/opt/jdk21/bin/java"
    os.unlink(f.name)


def test_local_jar_env_override(monkeypatch):
    monkeypatch.setenv("GSP_BACKEND_MODE", "local_jar")
    monkeypatch.setenv("GSP_JAR_PATH", "/env/gsp.jar")
    cfg = load_config(config_path=None)
    assert cfg.sqlflow.mode == "local_jar"
    assert cfg.sqlflow.jar_path == "/env/gsp.jar"


def test_effective_url_defaults():
    cfg = load_config(config_path=None)
    assert cfg.sqlflow.mode == "anonymous"
    assert "anonymous" in cfg.sqlflow.effective_url

    # Explicit URL should override default
    cfg.sqlflow.url = "http://custom:8080/api"
    assert cfg.sqlflow.effective_url == "http://custom:8080/api"
