"""Configuration loading with YAML file + environment variable overrides."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


# Default API URLs per mode
DEFAULT_URLS = {
    "anonymous": "https://api.gudusoft.com/gspLive_backend/api/anonymous/lineage",
    "authenticated": "https://api.gudusoft.com/gspLive_backend/v1/sqlflow/sqlflow/exportFullLineageAsJson",
    "self_hosted": "http://localhost:8081/gspLive_backend/v1/sqlflow/sqlflow/exportFullLineageAsJson",
}


@dataclass
class SQLFlowConfig:
    mode: str = "anonymous"
    url: Optional[str] = None
    secret_key: Optional[str] = None
    db_vendor: str = "dbvbigquery"
    show_relation_type: str = "fdd"

    @property
    def effective_url(self) -> str:
        """Return the explicit URL if set, otherwise the default for the mode."""
        if self.url:
            return self.url
        return DEFAULT_URLS[self.mode]


@dataclass
class DataHubConfig:
    server: str = "http://localhost:8080"
    token: Optional[str] = None
    platform: str = "bigquery"
    env: str = "PROD"


@dataclass
class LogParserConfig:
    log_file: Optional[str] = None
    sql_file: Optional[str] = None
    sql_text: Optional[str] = None


@dataclass
class SidecarConfig:
    sqlflow: SQLFlowConfig = field(default_factory=SQLFlowConfig)
    datahub: DataHubConfig = field(default_factory=DataHubConfig)
    log_parser: LogParserConfig = field(default_factory=LogParserConfig)


def load_config(config_path: Optional[str] = None) -> SidecarConfig:
    """Load configuration from YAML file, then override with environment variables.

    Priority (highest wins):
      1. Environment variables (GSP_BACKEND_MODE, GSP_SQLFLOW_URL, etc.)
      2. YAML config file
      3. Built-in defaults
    """
    cfg = SidecarConfig()

    # --- Load YAML if provided ---
    if config_path and Path(config_path).exists():
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}

        sf = raw.get("sqlflow", {})
        cfg.sqlflow.mode = sf.get("mode", cfg.sqlflow.mode)
        cfg.sqlflow.url = sf.get("url", cfg.sqlflow.url)
        cfg.sqlflow.secret_key = sf.get("secret_key", cfg.sqlflow.secret_key)
        cfg.sqlflow.db_vendor = sf.get("db_vendor", cfg.sqlflow.db_vendor)
        cfg.sqlflow.show_relation_type = sf.get("show_relation_type", cfg.sqlflow.show_relation_type)

        dh = raw.get("datahub", {})
        cfg.datahub.server = dh.get("server", cfg.datahub.server)
        cfg.datahub.token = dh.get("token", cfg.datahub.token)
        cfg.datahub.platform = dh.get("platform", cfg.datahub.platform)
        cfg.datahub.env = dh.get("env", cfg.datahub.env)

        lp = raw.get("log_parser", {})
        cfg.log_parser.log_file = lp.get("log_file", cfg.log_parser.log_file)
        cfg.log_parser.sql_file = lp.get("sql_file", cfg.log_parser.sql_file)
        cfg.log_parser.sql_text = lp.get("sql_text", cfg.log_parser.sql_text)

    # --- Environment variable overrides ---
    env_map = {
        "GSP_BACKEND_MODE": ("sqlflow", "mode"),
        "GSP_SQLFLOW_URL": ("sqlflow", "url"),
        "GSP_SQLFLOW_SECRET_KEY": ("sqlflow", "secret_key"),
        "GSP_DB_VENDOR": ("sqlflow", "db_vendor"),
        "GSP_SHOW_RELATION_TYPE": ("sqlflow", "show_relation_type"),
        "GSP_DATAHUB_SERVER": ("datahub", "server"),
        "GSP_DATAHUB_TOKEN": ("datahub", "token"),
        "GSP_DATAHUB_PLATFORM": ("datahub", "platform"),
        "GSP_DATAHUB_ENV": ("datahub", "env"),
        "GSP_LOG_FILE": ("log_parser", "log_file"),
        "GSP_SQL_FILE": ("log_parser", "sql_file"),
        "GSP_SQL_TEXT": ("log_parser", "sql_text"),
    }
    for env_var, (section, attr) in env_map.items():
        val = os.environ.get(env_var)
        if val is not None:
            setattr(getattr(cfg, section), attr, val)

    # --- Validate ---
    valid_modes = {"anonymous", "authenticated", "self_hosted"}
    if cfg.sqlflow.mode not in valid_modes:
        raise ValueError(
            f"Invalid sqlflow.mode '{cfg.sqlflow.mode}'. Must be one of: {valid_modes}"
        )

    if cfg.sqlflow.mode == "authenticated" and not cfg.sqlflow.secret_key:
        raise ValueError(
            "sqlflow.secret_key is required when mode is 'authenticated'. "
            "Get a key at https://docs.gudusoft.com/sign-up/"
        )

    return cfg
