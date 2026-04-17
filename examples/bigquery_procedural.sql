-- BigQuery procedural SQL from DataHub Issue #11654
-- https://github.com/datahub-project/datahub/issues/11654
--
-- This is the exact SQL that sqlglot cannot parse, causing DataHub
-- to lose lineage. GSP/SQLFlow parses it correctly and extracts
-- 25 column-level lineage relationships.
--
-- Test with:
--   gsp-datahub-sidecar --sql-file examples/bigquery_procedural.sql --dry-run

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

  -- Check if the delta table contains new data
  IF (SELECT COUNT(1) FROM temp_table_delta) > 0 THEN
    CREATE OR REPLACE TEMP TABLE final_output AS
    SELECT DISTINCT IDField, Email, UserID, EventTimestamp, BusinessDate
    FROM temp_table_delta
    WHERE EventTimestamp BETWEEN '2023-01-01' AND '2023-12-31';
  END IF;
END IF;
