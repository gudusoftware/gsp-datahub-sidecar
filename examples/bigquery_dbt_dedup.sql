-- BigQuery dbt deduplication macro — column-level lineage silently lost
-- https://github.com/datahub-project/datahub/issues/11670
--
-- dbt-utils' deduplicate macro generates this pattern. sqlglot can extract
-- table-level lineage but misses all column-level lineage due to the
-- array_agg()[offset(0)] + struct unpacking (unique.*) syntax.
-- GSP parses this BigQuery-specific syntax correctly.
--
-- Test with:
--   gsp-datahub-sidecar --sql-file examples/bigquery_dbt_dedup.sql --dry-run

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
);
