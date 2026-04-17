# gsp-datahub-sidecar

Recover BigQuery procedural-SQL lineage that DataHub's sqlglot parser misses, using [Gudu SQLFlow](https://gudusoft.com).

## The problem

DataHub's BigQuery ingestion uses sqlglot for SQL lineage extraction. sqlglot cannot parse **GoogleSQL Procedural Language** (`DECLARE`, `IF/THEN`, `CALL`, `CREATE TEMP TABLE` inside procedural blocks), causing lineage to silently break. See [datahub-project/datahub#11654](https://github.com/datahub-project/datahub/issues/11654).

## The solution

This sidecar runs alongside your existing DataHub ingestion. It re-parses the SQL statements that sqlglot failed on using Gudu SQLFlow (which handles procedural SQL natively), then emits the recovered lineage to DataHub via the REST API.

```
DataHub ingestion (unchanged)          gsp-datahub-sidecar (this tool)
  BQ audit log -> sqlglot -> lineage      |
                    |                     |  re-parse failed SQL with SQLFlow
                    v                     |  emit recovered lineage via REST
              "Command fallback"          v
              (lineage lost)        DataHub GMS (lineage restored)
```

## Quick start

```bash
pip install gsp-datahub-sidecar

# Analyze a SQL file (anonymous mode, no signup, 50 calls/day):
gsp-datahub-sidecar --sql-file queries.sql --dry-run

# Analyze inline SQL:
gsp-datahub-sidecar --sql "DECLARE x INT; CREATE VIEW v AS SELECT a FROM t" --dry-run

# Parse DataHub ingestion logs and emit lineage:
gsp-datahub-sidecar --config sidecar.yaml --log-file /var/log/datahub/ingest.log
```

## Three backend modes

| Mode | Auth | Limit | Data location | Use case |
|---|---|---|---|---|
| `anonymous` (default) | None | 50/day per IP | SQL sent to api.gudusoft.com | Quick evaluation |
| `authenticated` | Secret key | 500/day | SQL sent to api.gudusoft.com | Extended evaluation |
| `self_hosted` | Optional | Unlimited | SQL stays in your VPC | Production |

### Anonymous (default, zero setup)

```bash
gsp-datahub-sidecar --sql-file queries.sql --dry-run
```

### Authenticated (sign up for a free key)

Get a key at [gudusoft.com/sqlflow/get-key](https://gudusoft.com/sqlflow/get-key), then:

```bash
gsp-datahub-sidecar --mode authenticated --secret-key sk-your-key-here --sql-file queries.sql
```

### Self-hosted (production)

Deploy SQLFlow Docker in your VPC, then:

```bash
gsp-datahub-sidecar --mode self_hosted \
  --sqlflow-url http://sqlflow:8081/gspLive_backend/v1/sqlflow/sqlflow/exportFullLineageAsJson \
  --sql-file queries.sql
```

## Configuration

Copy `sidecar.yaml.example` to `sidecar.yaml` and edit. All settings can also be overridden with environment variables:

| Env var | Config key | Description |
|---|---|---|
| `GSP_BACKEND_MODE` | `sqlflow.mode` | `anonymous`, `authenticated`, or `self_hosted` |
| `GSP_SQLFLOW_URL` | `sqlflow.url` | Override the SQLFlow API URL |
| `GSP_SQLFLOW_SECRET_KEY` | `sqlflow.secret_key` | API key for authenticated mode |
| `GSP_DB_VENDOR` | `sqlflow.db_vendor` | SQL dialect (default: `dbvbigquery`) |
| `GSP_DATAHUB_SERVER` | `datahub.server` | DataHub GMS URL |
| `GSP_DATAHUB_TOKEN` | `datahub.token` | DataHub auth token |

## Licensing

This sidecar (glue code) is Apache 2.0 licensed. [Gudu SQLFlow](https://gudusoft.com) is a commercial product by Gudu Software. The anonymous tier provides free evaluation access. For production use, deploy the self-hosted SQLFlow Docker with a license.
