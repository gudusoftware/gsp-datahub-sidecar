-- MSSQL view with mixed-case identifiers that sqlglot lowercases incorrectly
-- https://github.com/datahub-project/datahub/issues/13792
-- https://github.com/datahub-project/datahub/issues/11322
--
-- sqlglot lowercases table and column names in lineage URNs, breaking the
-- match against schemaMetadata which preserves original casing. GSP's T-SQL
-- parser preserves identifier casing exactly as written.
--
-- Test with:
--   gsp-datahub-sidecar --sql-file examples/mssql_case_sensitivity.sql --db-vendor dbvmssql --dry-run

CREATE VIEW MDM.sales.ecosystem_partners
AS
SELECT
    PartnerID,
    PartnerName,
    ConnectionRole,
    RegionCode
FROM MDM.sales.MDM_CONNECTIONROLE_ECO_PARTNERS_MASTER
WHERE ActiveFlag = 1;
