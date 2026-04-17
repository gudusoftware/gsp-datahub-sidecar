"""Integration test against the real SQLFlow API.

Run with: pytest tests/test_integration.py -v
Requires: network access to api.gudusoft.com

These tests use the authenticated endpoint with a secret key.
Set GSP_SQLFLOW_SECRET_KEY env var or skip these tests.
"""

import os

import pytest

from gsp_datahub_sidecar.backend import AuthenticatedBackend
from gsp_datahub_sidecar.lineage_mapper import extract_lineage

SQLFLOW_URL = "https://api.gudusoft.com/gspLive_backend/v1/sqlflow/sqlflow/exportFullLineageAsJson"

# The exact SQL from DataHub Issue #11654
BIGQUERY_PROCEDURAL_SQL = """\
DECLARE current_job_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
DECLARE max_record_ts TIMESTAMP DEFAULT NULL;
DECLARE partitions STRUCT<max_record_ts TIMESTAMP, dates ARRAY<DATE>> DEFAULT NULL;

CALL `internal_project.get_partitions`(
  ('project.dataset.view_name', 'EventTimestamp'),
  ('project.dataset.other_table', 'BusinessDate', 'EventTimestamp'),
  partitions
);

IF ARRAY_LENGTH(partitions.dates) > 0 THEN
  CREATE OR REPLACE TEMP TABLE temp_table AS
  SELECT * EXCEPT (SnapshotTimestamp)
  FROM `project.dataset.view_name`
  WHERE (IDField, FlagField, ForeignKeyField, StartDate) IN UNNEST(partitions.dates)
  ORDER BY EventTimestamp;

  IF (SELECT COUNT(1) FROM temp_table_delta) > 0 THEN
    CREATE OR REPLACE TEMP TABLE final_output AS
    SELECT DISTINCT IDField, Email, UserID, EventTimestamp, BusinessDate
    FROM temp_table_delta
    WHERE EventTimestamp BETWEEN '2023-01-01' AND '2023-12-31';
  END IF;
END IF;
"""

ORACLE_VIEW_SQL = """\
CREATE VIEW vsal
AS
  SELECT a.deptno                  "Department",
         a.num_emp / b.total_count "Employees",
         a.sal_sum / b.total_sal   "Salary"
  FROM   (SELECT deptno,
                 Count()  num_emp,
                 SUM(sal) sal_sum
          FROM   scott.emp
          WHERE  city = 'NYC'
          GROUP  BY deptno) a,
         (SELECT Count()  total_count,
                 SUM(sal) total_sal
          FROM   scott.emp
          WHERE  city = 'NYC') b
"""


def get_secret_key():
    key = os.environ.get("GSP_SQLFLOW_SECRET_KEY")
    if not key:
        pytest.skip("GSP_SQLFLOW_SECRET_KEY not set — skipping integration tests")
    return key


class TestBigQueryProceduralSQL:
    """Test the exact SQL from DataHub Issue #11654."""

    def test_api_returns_200(self):
        backend = AuthenticatedBackend(url=SQLFLOW_URL, secret_key=get_secret_key())
        response = backend.get_lineage(BIGQUERY_PROCEDURAL_SQL, "dbvbigquery")
        assert response["code"] == 200

    def test_extracts_lineage(self):
        backend = AuthenticatedBackend(url=SQLFLOW_URL, secret_key=get_secret_key())
        response = backend.get_lineage(BIGQUERY_PROCEDURAL_SQL, "dbvbigquery")
        lineages = extract_lineage(response)

        # Should find at least one lineage flow
        assert len(lineages) > 0

        # Should find project.dataset.view_name as an upstream
        upstream_tables = {tl.upstream_table for tl in lineages}
        assert any("VIEW_NAME" in t.upper() for t in upstream_tables), \
            f"Expected VIEW_NAME in upstreams, got: {upstream_tables}"

    def test_column_level_lineage(self):
        backend = AuthenticatedBackend(url=SQLFLOW_URL, secret_key=get_secret_key())
        response = backend.get_lineage(BIGQUERY_PROCEDURAL_SQL, "dbvbigquery")
        lineages = extract_lineage(response)

        # Should have column-level detail
        total_columns = sum(len(tl.column_mappings) for tl in lineages)
        assert total_columns > 0, "Expected column-level lineage"


class TestOracleViewSQL:
    """Test with Oracle CREATE VIEW (baseline validation)."""

    def test_api_returns_200(self):
        backend = AuthenticatedBackend(url=SQLFLOW_URL, secret_key=get_secret_key())
        response = backend.get_lineage(ORACLE_VIEW_SQL, "dbvoracle")
        assert response["code"] == 200

    def test_extracts_lineage(self):
        backend = AuthenticatedBackend(url=SQLFLOW_URL, secret_key=get_secret_key())
        response = backend.get_lineage(ORACLE_VIEW_SQL, "dbvoracle")
        lineages = extract_lineage(response)

        # scott.emp -> vsal
        assert len(lineages) >= 1
        downstream_tables = {tl.downstream_table for tl in lineages}
        assert any("VSAL" in t.upper() for t in downstream_tables)
