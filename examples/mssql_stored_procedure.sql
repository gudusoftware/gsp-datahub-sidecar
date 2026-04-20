-- MSSQL stored procedure with CASE expression that breaks sqlglot's split_statements
-- https://github.com/datahub-project/datahub/issues/12606
--
-- sqlglot's split_statements incorrectly splits at the CASE...END keyword,
-- producing three broken fragments instead of one valid statement.
-- GSP parses the complete T-SQL statement correctly.
--
-- Test with:
--   gsp-datahub-sidecar --sql-file examples/mssql_stored_procedure.sql --db-vendor dbvmssql --dry-run

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
