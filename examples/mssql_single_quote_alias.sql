-- MSSQL single-quote column aliases — valid T-SQL syntax that sqlglot missed
-- https://github.com/datahub-project/datahub/issues/9843
--
-- T-SQL supports four alias syntaxes. sqlglot handled the first three but
-- failed on single-quoted aliases (c4 as 'c4_alias'). GSP supports all four.
--
-- Test with:
--   gsp-datahub-sidecar --sql-file examples/mssql_single_quote_alias.sql --db-vendor dbvmssql --dry-run

SELECT
    c1 as c1_alias,
    c2 as [c2_alias],
    c3 as "c3_alias",
    c4 as 'c4_alias'
FROM
    dbo.tbl;
