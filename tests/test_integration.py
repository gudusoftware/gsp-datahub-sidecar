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

# MSSQL stored procedure with CASE...END from DataHub Issue #12606
MSSQL_STORED_PROC_SQL = """\
CREATE PROCEDURE dbo.ClassifyRecords
AS
BEGIN
    SELECT
        Id,
        CASE
            WHEN Id > 10 THEN 'IS_GREATER_THAN_TEN'
            WHEN Id > 5 THEN 'IS_GREATER_THAN_FIVE'
            ELSE 'IS_SMALL'
        END AS foo,
        bar
    FROM mySchema.myTable;

    INSERT INTO mySchema.classified_results (Id, classification, bar)
    SELECT
        Id,
        CASE
            WHEN Id > 10 THEN 'IS_GREATER_THAN_TEN'
            WHEN Id > 5 THEN 'IS_GREATER_THAN_FIVE'
            ELSE 'IS_SMALL'
        END,
        bar
    FROM mySchema.myTable
    WHERE bar IS NOT NULL;
END;
"""

# MSSQL view with mixed-case identifiers from DataHub Issues #13792 / #11322
MSSQL_CASE_SENSITIVITY_SQL = """\
CREATE VIEW MDM.sales.ecosystem_partners
AS
SELECT
    PartnerID,
    PartnerName,
    ConnectionRole,
    RegionCode
FROM MDM.sales.MDM_CONNECTIONROLE_ECO_PARTNERS_MASTER
WHERE ActiveFlag = 1;
"""

# BigQuery dbt deduplication macro from DataHub Issue #11670
BIGQUERY_DBT_DEDUP_SQL = """\
CREATE VIEW analytics.deduplicated_articles AS
select unique.*
from (
     select
         array_agg (
                 original
                     order by article_name desc
            limit 1
         )[offset(0)] unique
     from all_articles original
     group by id
)
"""

# Power BI query with SQL comments from DataHub Issue #11251
POWERBI_COMMENTS_SQL = """\
CREATE VIEW dbo.customer_branches AS
select
upper(cs.customercode) as customercode
, cs.ear2id as ear2id
, db.branch_rollup_name
 , db.subregion1
 , db.subregion2
from nast_truckload_domain.broker.dim_customer cs
--join nast_customer_domain.broker.dim_customer_ear2_single_ownership_history as so on cs.ear2id = so.ear2_id and is_current = true
join nast_customer_domain.broker.dim_customer_ear2_single_ownership_history as so on cs.ear2id = so.ear2_id and is_current = true
join nast_customer_domain.broker.ref_branch db on db.Branch_code = so.branch_code
-- join nast_customer_domain.broker.customer_long_term_value as clv on cs.ear2id = clv.ear2_id
where cs.customerstatusid = 1 --active
and db.primary_business_line_id in  ('62','73')
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


class TestMSSQLStoredProcedure:
    """Test MSSQL stored procedure with CASE...END (DataHub Issue #12606).

    sqlglot's split_statements incorrectly splits at the CASE...END keyword.
    GSP should parse the entire procedure as a single unit.
    """

    def test_api_returns_200(self):
        backend = AuthenticatedBackend(url=SQLFLOW_URL, secret_key=get_secret_key())
        response = backend.get_lineage(MSSQL_STORED_PROC_SQL, "dbvmssql")
        assert response["code"] == 200

    def test_extracts_lineage(self):
        backend = AuthenticatedBackend(url=SQLFLOW_URL, secret_key=get_secret_key())
        response = backend.get_lineage(MSSQL_STORED_PROC_SQL, "dbvmssql")
        lineages = extract_lineage(response)

        assert len(lineages) > 0
        # mySchema.myTable should appear as upstream
        upstream_tables = {tl.upstream_table for tl in lineages}
        assert any("MYTABLE" in t.upper() for t in upstream_tables), \
            f"Expected MYTABLE in upstreams, got: {upstream_tables}"

    def test_column_level_lineage(self):
        backend = AuthenticatedBackend(url=SQLFLOW_URL, secret_key=get_secret_key())
        response = backend.get_lineage(MSSQL_STORED_PROC_SQL, "dbvmssql")
        lineages = extract_lineage(response)

        total_columns = sum(len(tl.column_mappings) for tl in lineages)
        assert total_columns > 0, "Expected column-level lineage for MSSQL stored proc"


class TestMSSQLCaseSensitivity:
    """Test MSSQL view with mixed-case identifiers (DataHub Issues #13792, #11322).

    sqlglot lowercases identifiers in lineage URNs, breaking the match
    against schemaMetadata. GSP should preserve original casing.
    """

    def test_api_returns_200(self):
        backend = AuthenticatedBackend(url=SQLFLOW_URL, secret_key=get_secret_key())
        response = backend.get_lineage(MSSQL_CASE_SENSITIVITY_SQL, "dbvmssql")
        assert response["code"] == 200

    def test_extracts_lineage(self):
        backend = AuthenticatedBackend(url=SQLFLOW_URL, secret_key=get_secret_key())
        response = backend.get_lineage(MSSQL_CASE_SENSITIVITY_SQL, "dbvmssql")
        lineages = extract_lineage(response)

        assert len(lineages) >= 1
        # MDM_CONNECTIONROLE_ECO_PARTNERS_MASTER -> ecosystem_partners
        downstream_tables = {tl.downstream_table for tl in lineages}
        assert any("ECOSYSTEM_PARTNERS" in t.upper() for t in downstream_tables), \
            f"Expected ECOSYSTEM_PARTNERS in downstreams, got: {downstream_tables}"


class TestBigQueryDbtDedup:
    """Test BigQuery dbt deduplication macro (DataHub Issue #11670).

    sqlglot gets table-level lineage but misses all column-level lineage
    due to array_agg()[offset(0)] + struct unpacking.
    """

    def test_api_returns_200(self):
        backend = AuthenticatedBackend(url=SQLFLOW_URL, secret_key=get_secret_key())
        response = backend.get_lineage(BIGQUERY_DBT_DEDUP_SQL, "dbvbigquery")
        assert response["code"] == 200

    def test_extracts_lineage(self):
        backend = AuthenticatedBackend(url=SQLFLOW_URL, secret_key=get_secret_key())
        response = backend.get_lineage(BIGQUERY_DBT_DEDUP_SQL, "dbvbigquery")
        lineages = extract_lineage(response)

        assert len(lineages) > 0
        # all_articles should appear as upstream
        upstream_tables = {tl.upstream_table for tl in lineages}
        assert any("ALL_ARTICLES" in t.upper() for t in upstream_tables), \
            f"Expected ALL_ARTICLES in upstreams, got: {upstream_tables}"


class TestPowerBIComments:
    """Test Power BI query with SQL comments (DataHub Issue #11251).

    sqlglot silently drops all JOINs and WHERE clauses after encountering
    a -- comment. GSP handles comment stripping correctly.
    """

    def test_api_returns_200(self):
        backend = AuthenticatedBackend(url=SQLFLOW_URL, secret_key=get_secret_key())
        response = backend.get_lineage(POWERBI_COMMENTS_SQL, "dbvmssql")
        assert response["code"] == 200

    def test_extracts_all_tables(self):
        backend = AuthenticatedBackend(url=SQLFLOW_URL, secret_key=get_secret_key())
        response = backend.get_lineage(POWERBI_COMMENTS_SQL, "dbvmssql")
        lineages = extract_lineage(response)

        # Should find all three active tables (not commented-out ones)
        all_tables = set()
        for tl in lineages:
            all_tables.add(tl.upstream_table.upper())
            all_tables.add(tl.downstream_table.upper())
        # dim_customer, dim_customer_ear2_single_ownership_history, ref_branch
        assert any("DIM_CUSTOMER" in t for t in all_tables), \
            f"Expected DIM_CUSTOMER in tables, got: {all_tables}"
