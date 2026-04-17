# gsp-datahub-sidecar

Recover BigQuery procedural-SQL lineage that DataHub's sqlglot parser misses, using [Gudu SQLFlow](https://sqlflow.gudusoft.com).

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

## Install

```bash
pip install git+https://github.com/gudusoftware/gsp-datahub-sidecar.git
```

If you hit `error: externally-managed-environment` on Ubuntu 23.04+ / Debian 12+, see [Troubleshooting](#troubleshooting).

## Quick start

Sample SQL files are included in the `examples/` directory — try them immediately after install:

```bash
# Try the BigQuery procedural SQL from DataHub Issue #11654:
gsp-datahub-sidecar --sql-file examples/bigquery_procedural.sql --dry-run

# Try an Oracle CREATE VIEW with subqueries:
gsp-datahub-sidecar --sql-file examples/oracle_create_view.sql --db-vendor dbvoracle --dry-run

# Analyze inline SQL:
gsp-datahub-sidecar --sql "DECLARE x INT; CREATE VIEW v AS SELECT a FROM t" --dry-run

# Parse DataHub ingestion logs and emit lineage:
gsp-datahub-sidecar --config sidecar.yaml --log-file /var/log/datahub/ingest.log
```

## Three backend modes

| Mode | Auth | Limit | Data location | Use case |
|---|---|---|---|---|
| `anonymous` (default) | None | 50/day per IP | SQL sent to api.gudusoft.com | Quick evaluation |
| `authenticated` | Secret key | 10k/month | SQL sent to api.gudusoft.com | Extended evaluation |
| `self_hosted` | `userId` + `secretKey` (token-exchange) | Unlimited | SQL stays in your VPC | Production |

### Anonymous (default, zero setup)

```bash
gsp-datahub-sidecar --sql-file queries.sql --dry-run
```

### Authenticated (sign up for a free key)

Get a key at [docs.gudusoft.com/sign-up](https://docs.gudusoft.com/sign-up/), then:

```bash
gsp-datahub-sidecar --mode authenticated --secret-key sk-your-key-here --sql-file queries.sql
```

### Self-hosted (production)

[Deploy SQLFlow Docker](https://docs.gudusoft.com/docker/) in your VPC, grab your `userId` and `secretKey` from the SQLFlow web UI (e.g. `http://localhost:8165/`), then:

```bash
gsp-datahub-sidecar --mode self_hosted \
  --sqlflow-url http://localhost:8165/api/gspLive_backend/sqlflow/generation/sqlflow/exportFullLineageAsJson \
  --user-id YOUR_USER_ID \
  --secret-key YOUR_SECRET_KEY \
  --sql-file queries.sql
```

**How auth works.** The sidecar implements SQLFlow Docker's two-step protocol automatically:

1. `POST /api/gspLive_backend/user/generateToken` with form-encoded `userId` + `secretKey` → returns a short-lived JWT `token`.
2. `POST …/exportFullLineageAsJson` with form-encoded `userId` + `token` (never the `secretKey` itself).

The token is cached for the process lifetime and silently refreshed if the server returns `code: 401`. For reference, this matches SQLFlow's [GenerateToken.py](https://github.com/sqlparser/sqlflow_public/blob/master/api/python/basic/GenerateToken.py).

Omit `--user-id` / `--secret-key` only if your Docker image is configured with auth disabled.

## Configuration

Copy `sidecar.yaml.example` to `sidecar.yaml` and edit. All settings can also be overridden with environment variables:

| Env var | Config key | Description |
|---|---|---|
| `GSP_BACKEND_MODE` | `sqlflow.mode` | `anonymous`, `authenticated`, or `self_hosted` |
| `GSP_SQLFLOW_URL` | `sqlflow.url` | Override the SQLFlow API URL |
| `GSP_SQLFLOW_USER_ID` | `sqlflow.user_id` | SQLFlow userId (self_hosted Docker with auth enabled) |
| `GSP_SQLFLOW_SECRET_KEY` | `sqlflow.secret_key` | API key for authenticated or self_hosted mode |
| `GSP_DB_VENDOR` | `sqlflow.db_vendor` | SQL dialect (default: `dbvbigquery`) |
| `GSP_DATAHUB_SERVER` | `datahub.server` | DataHub GMS URL |
| `GSP_DATAHUB_TOKEN` | `datahub.token` | DataHub auth token |

## Dry run vs. live mode

`--dry-run` does everything **except** sending lineage to DataHub — safe to run anytime, no DataHub server needed:

```bash
# Dry run: parse SQL, extract lineage, show what would be sent (no DataHub needed)
gsp-datahub-sidecar --sql-file examples/bigquery_procedural.sql --dry-run
```

Without `--dry-run`, lineage is written to a running DataHub GMS. Point to your DataHub server with `--datahub-server`:

```bash
# Emit lineage to DataHub running on your cluster:
gsp-datahub-sidecar --sql-file examples/bigquery_procedural.sql \
  --datahub-server http://datahub-gms:8080

# If DataHub has authentication enabled, add a token:
gsp-datahub-sidecar --sql-file examples/bigquery_procedural.sql \
  --datahub-server http://datahub-gms:8080 \
  --datahub-token eyJhbGciOi...
```

Or set these in `sidecar.yaml` / environment variables so you don't repeat them:

```bash
export GSP_DATAHUB_SERVER=http://datahub-gms:8080
export GSP_DATAHUB_TOKEN=eyJhbGciOi...
gsp-datahub-sidecar --sql-file examples/bigquery_procedural.sql
```

| Step | `--dry-run` | live (no flag) |
|---|---|---|
| Call SQLFlow API to parse SQL | Yes | Yes |
| Extract lineage from response | Yes | Yes |
| Build DataHub MCPs | Yes | Yes |
| **Send MCPs to DataHub GMS** | **No** (logs what it would send) | **Yes** |

## Verify lineage in DataHub

After emitting lineage (live mode, no `--dry-run`), verify it in three ways:

**1. DataHub Web UI (recommended)**

Open DataHub in your browser (e.g. `http://datahub-frontend:9002`), search for the downstream table name, and click the **Lineage** tab. You should see arrows connecting upstream tables to downstream tables.

For the BigQuery procedural example, search for `temp_table` or `final_output`:

```
project.dataset.view_name  ──>  temp_table       (6 columns)
temp_table_delta           ──>  final_output     (5 columns)
```

**2. DataHub CLI**

```bash
# Check lineage for a specific dataset:
datahub get --urn "urn:li:dataset:(urn:li:dataPlatform:bigquery,temp_table,PROD)" --aspect upstreamLineage
```

**3. DataHub GMS REST API**

```bash
# Query the lineage aspect directly:
curl -s "http://datahub-gms:8080/aspects/urn%3Ali%3Adataset%3A(urn%3Ali%3AdataPlatform%3Abigquery%2Ctemp_table%2CPROD)?aspect=upstreamLineage" | python3 -m json.tool
```

If the lineage appears, the sidecar successfully recovered what sqlglot missed.

## Fresh DataHub vs. existing DataHub

### Fresh DataHub (no existing metadata)

The sidecar works on a completely empty DataHub — DataHub auto-creates dataset entities when it receives lineage MCPs. You'll see the lineage graph immediately. However, the tables will appear as minimal shells:

| | With BigQuery ingestion | Sidecar only (no prior metadata) |
|---|---|---|
| Lineage arrows in graph | Yes | Yes |
| Column-level lineage | Yes | Yes |
| Table/column names | Full display names | URN-derived names |
| Column types & descriptions | Yes | No |
| Table schema / field list | Yes | No |
| Row counts / statistics | Yes | No |
| Platform icon (BigQuery logo) | Yes | Yes |

This is fine for a **demo or evaluation** — the lineage visualization proves that GSP recovers what sqlglot misses. The missing metadata would normally come from DataHub's BigQuery ingestion.

### Existing DataHub (with metadata and sqlglot-generated lineage)

This is the real production scenario. The sidecar **adds to** the lineage that DataHub's BigQuery ingestion already created — it does not replace or conflict with it.

How it works:

- DataHub's BigQuery ingestion runs first and creates lineage for all SQL that sqlglot **can** parse (standard SELECT, INSERT, CREATE VIEW, etc.)
- The sidecar runs after and emits lineage only for the SQL that sqlglot **failed** on (procedural blocks with DECLARE, IF/THEN, CALL, etc.)
- DataHub merges both into a single lineage graph per dataset

The result is a **more complete** lineage graph — the existing lineage stays intact, and the sidecar fills in the gaps.

**Important: URN matching**

For the sidecar's lineage to connect with existing DataHub entities, the dataset URNs must match exactly. This means the table names in the sidecar output must match the names DataHub's BigQuery ingestion created.

Things to check:

1. **Case sensitivity**: DataHub's BigQuery ingestion lowercases URNs when `convert_urns_to_lowercase: true` is set in the ingestion config (which is common). The sidecar also lowercases by default, so this should match. If your DataHub uses mixed case, check that the URNs align.

2. **Project/dataset prefix**: BigQuery tables are typically ingested as `project.dataset.table`. The sidecar uses the table names as they appear in the SQL. If your SQL uses backtick-quoted names like `` `project.dataset.table` ``, the sidecar strips the backticks and preserves the full path. Verify that the resulting URN matches what DataHub already has.

3. **Platform and environment**: The sidecar defaults to `platform: bigquery` and `env: PROD`. If your DataHub uses different values (e.g. `env: DEV`), set them in `sidecar.yaml` or via `--datahub-platform` / `GSP_DATAHUB_ENV` to match.

4. **Temp tables**: The sidecar emits lineage for temp tables (e.g. `temp_table`, `final_output`). If your DataHub's `dataset_pattern.deny` excludes temp tables (common in BigQuery ingestion configs), the temp table entities will be created by the sidecar but won't have schema metadata. This is expected — the lineage through them is still valuable.

**The sidecar will NOT:**
- Overwrite or delete existing lineage created by DataHub's ingestion
- Modify any existing dataset metadata (schemas, descriptions, tags)
- Interfere with DataHub's ingestion schedule or stateful ingestion

## Troubleshooting

### `error: externally-managed-environment` when running `pip install`

On Ubuntu 23.04+ / Debian 12+ (Python 3.11+), the system Python is [PEP 668](https://peps.python.org/pep-0668/)-protected to prevent `pip` from clobbering OS packages. You'll see:

```
error: externally-managed-environment

× This environment is externally managed
╰─> To install Python packages system-wide, try apt install
    python3-xyz, where xyz is the package you are trying to
    install.
```

Pick one of the following:

**Option 1 — venv (recommended)**

```bash
apt install -y python3-venv python3-full
python3 -m venv ~/sqlflow-venv
~/sqlflow-venv/bin/pip install git+https://github.com/gudusoftware/gsp-datahub-sidecar.git
~/sqlflow-venv/bin/gsp-datahub-sidecar --help
```

Activate the venv (`source ~/sqlflow-venv/bin/activate`) or invoke the binary by its full path.

**Option 2 — pipx (isolated, auto-added to PATH)**

```bash
apt install -y pipx
pipx ensurepath
pipx install git+https://github.com/gudusoftware/gsp-datahub-sidecar.git
```

**Option 3 — override (quick, not recommended on a server)**

```bash
pip install --break-system-packages git+https://github.com/gudusoftware/gsp-datahub-sidecar.git
```

This can conflict with `apt`-managed Python packages. Use venv or pipx for anything long-lived.

### `Self-hosted SQLFlow returned HTTP 400` / `401 Invalid user or token, access deny`

The self-hosted SQLFlow Docker uses a two-step auth flow:

1. Exchange `userId` + `secretKey` for a short-lived `token` at `/api/gspLive_backend/user/generateToken` (form-encoded).
2. Call the lineage endpoint with `userId` + `token` (form-encoded, **not** JSON, **not** `secretKey`, **not** a Bearer header).

See SQLFlow's reference: [GenerateToken.py](https://github.com/sqlparser/sqlflow_public/blob/master/api/python/basic/GenerateToken.py).

Newer sidecar versions do this automatically — upgrade if you still see `HTTP 400` or `401 Invalid user or token`:

```bash
pip install --upgrade git+https://github.com/gudusoftware/gsp-datahub-sidecar.git
```

Then pass your SQLFlow Docker credentials (get them from the SQLFlow web UI at `http://<host>:8165/`):

```bash
gsp-datahub-sidecar --mode self_hosted \
  --sqlflow-url http://localhost:8165/api/gspLive_backend/sqlflow/generation/sqlflow/exportFullLineageAsJson \
  --user-id YOUR_USER_ID \
  --secret-key YOUR_SECRET_KEY \
  --sql-file queries.sql
```

You can verify the flow directly with `curl` first:

```bash
# 1. Generate a token
TOKEN=$(curl -sS -X POST \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode 'userId=YOUR_USER_ID' \
  --data-urlencode 'secretKey=YOUR_SECRET_KEY' \
  http://localhost:8165/api/gspLive_backend/user/generateToken \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')

# 2. Call the lineage endpoint with userId + token (not secretKey)
curl -sS -X POST \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode 'userId=YOUR_USER_ID' \
  --data-urlencode "token=$TOKEN" \
  --data-urlencode 'dbvendor=dbvbigquery' \
  --data-urlencode 'sqltext=SELECT a FROM t;' \
  --data-urlencode 'showRelationType=fdd' \
  http://localhost:8165/api/gspLive_backend/sqlflow/generation/sqlflow/exportFullLineageAsJson
```

If curl returns `{"code":200,...}` but the sidecar still fails, double-check that you upgraded past the version where the token flow was added.

### `FileNotFoundError: SQL file not found: examples/bigquery_procedural.sql`

`pip install git+https://...` only installs the Python package — it does **not** copy the `examples/` directory onto your filesystem. You'll see:

```
FileNotFoundError: SQL file not found: examples/bigquery_procedural.sql
```

Pick one:

**Option A — download a single example**

```bash
curl -L -O https://raw.githubusercontent.com/gudusoftware/gsp-datahub-sidecar/main/examples/bigquery_procedural.sql
curl -L -O https://raw.githubusercontent.com/gudusoftware/gsp-datahub-sidecar/main/examples/oracle_create_view.sql

gsp-datahub-sidecar --sql-file bigquery_procedural.sql --dry-run
```

**Option B — clone the repo for all examples**

```bash
git clone https://github.com/gudusoftware/gsp-datahub-sidecar.git
cd gsp-datahub-sidecar
gsp-datahub-sidecar --sql-file examples/bigquery_procedural.sql --dry-run
```

**Option C — pass SQL inline (no file needed)**

```bash
gsp-datahub-sidecar --sql "DECLARE x INT64; CREATE VIEW v AS SELECT a FROM t" --dry-run
```

**Option D — point at your own SQL file**

```bash
gsp-datahub-sidecar --sql-file /path/to/your/query.sql --dry-run
```

## Licensing

This sidecar (glue code) is Apache 2.0 licensed. [Gudu SQLFlow](https://sqlflow.gudusoft.com) is a commercial product by Gudu Software. The anonymous tier provides free evaluation access. For production use, deploy the self-hosted SQLFlow Docker with a license.
