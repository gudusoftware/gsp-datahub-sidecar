"""End-to-end test: authenticated mode + dry-run on examples/bigquery_procedural.sql.

Drives the full pipeline (config -> authenticated backend -> live SQLFlow API ->
lineage_mapper -> build_mcps -> emit_to_datahub(dry_run=True)) and asserts
that the expected column-level lineage is produced.

Credentials are read from a repo-root ``.env`` file (see ``.env`` template).
The test is skipped if ``.env`` is missing or credentials are blank, so CI
without secrets does not fail.

Run with:
    pytest tests/test_authenticated_column_lineage.py -v
"""

import os
from pathlib import Path

import pytest

from gsp_datahub_sidecar.backend import create_backend
from gsp_datahub_sidecar.config import load_config
from gsp_datahub_sidecar.emitter import build_mcps, emit_to_datahub
from gsp_datahub_sidecar.lineage_mapper import extract_lineage

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env"
SQL_FILE = REPO_ROOT / "examples" / "bigquery_procedural.sql"


def _load_dotenv(path: Path) -> dict[str, str]:
    """Minimal .env parser — no python-dotenv dependency.

    Supports ``KEY=VALUE`` lines, ignores blank lines and ``#`` comments.
    Values are taken verbatim (no quote stripping, no interpolation) — the
    raw string is what the SQLFlow API expects.
    """
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip()
    return out


@pytest.fixture(scope="module")
def auth_env():
    """Load .env and apply to os.environ so load_config() picks it up.

    Skips the module if credentials are absent — matches how the other
    integration tests behave when secrets are not available.
    """
    env = _load_dotenv(ENV_FILE)
    user_id = env.get("GSP_SQLFLOW_USER_ID") or os.environ.get("GSP_SQLFLOW_USER_ID")
    secret_key = env.get("GSP_SQLFLOW_SECRET_KEY") or os.environ.get("GSP_SQLFLOW_SECRET_KEY")
    if not user_id or not secret_key:
        pytest.skip(
            f"{ENV_FILE} missing or GSP_SQLFLOW_USER_ID / GSP_SQLFLOW_SECRET_KEY blank — "
            "skipping authenticated-mode live test."
        )

    prev = {
        k: os.environ.get(k)
        for k in ("GSP_BACKEND_MODE", "GSP_SQLFLOW_USER_ID", "GSP_SQLFLOW_SECRET_KEY")
    }
    os.environ["GSP_BACKEND_MODE"] = "authenticated"
    os.environ["GSP_SQLFLOW_USER_ID"] = user_id
    os.environ["GSP_SQLFLOW_SECRET_KEY"] = secret_key

    yield {"user_id": user_id, "secret_key": secret_key}

    for k, v in prev.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


@pytest.fixture(scope="module")
def lineages(auth_env):
    """Run the authenticated-mode pipeline once; share the result."""
    assert SQL_FILE.exists(), f"expected SQL fixture at {SQL_FILE}"
    sql = SQL_FILE.read_text()

    cfg = load_config(config_path=None)
    assert cfg.sqlflow.mode == "authenticated"
    assert cfg.sqlflow.user_id == auth_env["user_id"]

    backend = create_backend(cfg.sqlflow)
    response = backend.get_lineage(
        sql=sql,
        db_vendor=cfg.sqlflow.db_vendor,
        show_relation_type=cfg.sqlflow.show_relation_type,
    )
    assert response.get("code") == 200, f"SQLFlow returned non-200: {response}"
    return extract_lineage(response)


def test_two_table_lineages(lineages):
    """bigquery_procedural.sql produces exactly two persistent flows:

    1. project.dataset.view_name -> temp_table  (outer CTAS)
    2. temp_table_delta -> final_output         (inner CTAS)
    """
    pairs = {(tl.upstream_table.upper(), tl.downstream_table.upper()) for tl in lineages}
    assert ("PROJECT.DATASET.VIEW_NAME", "TEMP_TABLE") in pairs
    assert ("TEMP_TABLE_DELTA", "FINAL_OUTPUT") in pairs
    assert len(lineages) == 2, f"expected 2 lineages, got {len(lineages)}: {pairs}"


def test_final_output_column_lineage(lineages):
    """temp_table_delta -> final_output has exactly the 5 SELECT DISTINCT columns."""
    final = next(
        tl for tl in lineages
        if tl.downstream_table.upper() == "FINAL_OUTPUT"
    )
    mappings = {(s.upper(), t.upper()) for s, t in final.column_mappings if s != "*" and t != "*"}

    expected = {
        ("IDFIELD", "IDFIELD"),
        ("EMAIL", "EMAIL"),
        ("USERID", "USERID"),
        ("EVENTTIMESTAMP", "EVENTTIMESTAMP"),
        ("BUSINESSDATE", "BUSINESSDATE"),
    }
    assert mappings == expected, (
        f"final_output column mappings differ.\n"
        f"  missing: {expected - mappings}\n"
        f"  extra:   {mappings - expected}"
    )


def test_temp_table_column_lineage(lineages):
    """view_name -> temp_table has exactly 6 named column mappings.

    The SQL is ``SELECT * EXCEPT (SnapshotTimestamp) FROM view_name``. SQLFlow
    expands ``*`` into per-column fdd relationships; those named edges (after
    filtering the raw ``*`` wildcards produced alongside them) cover:
      - the 4 columns in the WHERE/ORDER BY clauses
      - SnapshotTimestamp (even though EXCEPT'd at execution, the parser
        still tracks the column-to-column relationship)
    for a total of 6 non-wildcard mappings.
    """
    temp = next(
        tl for tl in lineages
        if tl.downstream_table.upper() == "TEMP_TABLE"
    )
    mappings = {(s.upper(), t.upper()) for s, t in temp.column_mappings if s != "*" and t != "*"}

    expected = {
        ("IDFIELD", "IDFIELD"),
        ("FLAGFIELD", "FLAGFIELD"),
        ("FOREIGNKEYFIELD", "FOREIGNKEYFIELD"),
        ("STARTDATE", "STARTDATE"),
        ("EVENTTIMESTAMP", "EVENTTIMESTAMP"),
        ("SNAPSHOTTIMESTAMP", "SNAPSHOTTIMESTAMP"),
    }
    assert mappings == expected, (
        f"temp_table column mappings differ.\n"
        f"  missing: {expected - mappings}\n"
        f"  extra:   {mappings - expected}"
    )


def test_dry_run_emits_expected_mcps(lineages, caplog):
    """The dry-run emitter walks every MCP without touching the network.

    Confirms that build_mcps + emit_to_datahub(dry_run=True) on the real
    SQLFlow response produces:
      - a dataset-key MCP for every referenced table (4: 2 upstream + 2 downstream)
      - a schema-metadata MCP for every table with column lineage (4)
      - an upstream-lineage MCP per downstream table (2)
    """
    import logging

    cfg = load_config(config_path=None)
    # Force column lineage on; dry-run regardless of datahub.server being unreachable.
    cfg.datahub.column_lineage = True

    mcps = build_mcps(
        lineages,
        platform=cfg.datahub.platform,
        env=cfg.datahub.env,
        column_lineage=True,
    )

    # 4 datasetKey + 4 schemaMetadata + 2 upstreamLineage = 10
    aspect_names = [type(m.aspect).__name__ for m in mcps]
    assert aspect_names.count("DatasetKeyClass") == 4
    assert aspect_names.count("SchemaMetadataClass") == 4
    assert aspect_names.count("UpstreamLineageClass") == 2

    # Total fine-grained column lineages after wildcard filtering:
    # 6 (view_name -> temp_table) + 5 (temp_table_delta -> final_output) = 11
    total_fine_grained = sum(
        len(m.aspect.fineGrainedLineages or [])
        for m in mcps
        if type(m.aspect).__name__ == "UpstreamLineageClass"
    )
    assert total_fine_grained == 11, (
        f"expected 11 fine-grained column lineages (6 + 5), got {total_fine_grained}"
    )

    with caplog.at_level(logging.INFO, logger="gsp_datahub_sidecar.emitter"):
        emitted = emit_to_datahub(mcps, cfg.datahub, dry_run=True)
    assert emitted == len(mcps)
    # Dry-run printer must log at least one fine-grained column lineage line.
    assert any("->" in rec.getMessage() and "schemaField" in rec.getMessage()
               for rec in caplog.records), \
        "dry-run output should include at least one column-level arrow"
