"""CLI entry point for gsp-datahub-sidecar."""

import argparse
import json
import logging
import sys

from . import __version__
from .backend import RateLimitError, SQLFlowError, create_backend
from .config import load_config
from .emitter import build_mcps, emit_to_datahub
from .lineage_mapper import extract_lineage
from .log_parser import parse_log_file, parse_sql_file, parse_sql_text


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    parser = argparse.ArgumentParser(
        prog="gsp-datahub-sidecar",
        description=(
            "Recover BigQuery procedural-SQL lineage for DataHub.\n\n"
            "Parses SQL statements that sqlglot fails on (DECLARE, IF/THEN, CALL, etc.) "
            "using Gudu SQLFlow, and emits the lineage to DataHub."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # Analyze a SQL file (anonymous mode, no signup):\n"
            "  gsp-datahub-sidecar --sql-file queries.sql --dry-run\n\n"
            "  # Analyze inline SQL:\n"
            '  gsp-datahub-sidecar --sql "DECLARE x INT; CREATE VIEW v AS SELECT a FROM t"\n\n'
            "  # Parse DataHub ingestion logs and emit lineage:\n"
            "  gsp-datahub-sidecar --config sidecar.yaml --log-file /var/log/datahub/ingest.log\n\n"
            "  # Use authenticated mode with a personal key:\n"
            "  GSP_BACKEND_MODE=authenticated GSP_SQLFLOW_SECRET_KEY=sk-xxx gsp-datahub-sidecar --sql-file q.sql\n\n"
            "  # Use self-hosted Docker:\n"
            "  gsp-datahub-sidecar --config sidecar.yaml --mode self_hosted --sqlflow-url http://sqlflow:8081/gspLive_backend/v1/sqlflow/sqlflow/exportFullLineageAsJson\n"
        ),
    )

    # --- Input sources (mutually exclusive) ---
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument(
        "--log-file",
        help="Path to DataHub ingestion log file. Extracts SQL statements that sqlglot failed on.",
    )
    input_group.add_argument(
        "--sql-file",
        help="Path to a SQL file to analyze directly (bypass log parsing).",
    )
    input_group.add_argument(
        "--sql",
        help="Inline SQL text to analyze.",
    )

    # --- Config ---
    parser.add_argument(
        "--config", "-c",
        help="Path to sidecar.yaml config file (default: ./sidecar.yaml).",
        default="sidecar.yaml",
    )

    # --- CLI overrides (take precedence over config file) ---
    parser.add_argument(
        "--mode",
        choices=["anonymous", "authenticated", "self_hosted"],
        help="SQLFlow backend mode (overrides config file).",
    )
    parser.add_argument(
        "--sqlflow-url",
        help="SQLFlow API URL (overrides config file).",
    )
    parser.add_argument(
        "--secret-key",
        help="SQLFlow secret key for authenticated mode (overrides config file).",
    )
    parser.add_argument(
        "--db-vendor",
        help="SQL dialect (default: dbvbigquery).",
    )

    # --- DataHub ---
    parser.add_argument(
        "--datahub-server",
        help="DataHub GMS server URL (overrides config file).",
    )
    parser.add_argument(
        "--datahub-token",
        help="DataHub auth token (overrides config file).",
    )

    # --- Output control ---
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and analyze SQL but don't emit to DataHub. Shows what would be sent.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output raw SQLFlow lineage JSON to stdout (useful for debugging).",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)
    logger = logging.getLogger("gsp_datahub_sidecar")

    # --- Load config ---
    config = load_config(args.config)

    # --- Apply CLI overrides ---
    if args.mode:
        config.sqlflow.mode = args.mode
    if args.sqlflow_url:
        config.sqlflow.url = args.sqlflow_url
    if args.secret_key:
        config.sqlflow.secret_key = args.secret_key
    if args.db_vendor:
        config.sqlflow.db_vendor = args.db_vendor
    if args.datahub_server:
        config.datahub.server = args.datahub_server
    if args.datahub_token:
        config.datahub.token = args.datahub_token
    if args.log_file:
        config.log_parser.log_file = args.log_file
    if args.sql_file:
        config.log_parser.sql_file = args.sql_file
    if args.sql:
        config.log_parser.sql_text = args.sql

    # --- Determine input source ---
    if config.log_parser.sql_text:
        statements = parse_sql_text(config.log_parser.sql_text)
    elif config.log_parser.sql_file:
        statements = parse_sql_file(config.log_parser.sql_file)
    elif config.log_parser.log_file:
        statements = parse_log_file(config.log_parser.log_file)
    else:
        logger.error(
            "No input provided. Use --sql, --sql-file, or --log-file.\n"
            "Run with --help for usage examples."
        )
        sys.exit(1)

    if not statements:
        logger.info("No SQL statements to process.")
        sys.exit(0)

    logger.info("Processing %d SQL statement(s) in '%s' mode...",
                len(statements), config.sqlflow.mode)

    # --- Create backend ---
    try:
        backend = create_backend(config.sqlflow)
    except ValueError as e:
        logger.error("Configuration error: %s", e)
        sys.exit(1)

    # --- Process each statement ---
    all_lineages = []
    errors = 0

    for i, stmt in enumerate(statements, 1):
        logger.info("[%d/%d] Analyzing SQL (%d chars) from %s...",
                    i, len(statements), len(stmt.sql), stmt.source)

        try:
            response = backend.get_lineage(
                sql=stmt.sql,
                db_vendor=config.sqlflow.db_vendor,
                show_relation_type=config.sqlflow.show_relation_type,
            )

            if args.output_json:
                print(json.dumps(response, indent=2))

            # Check response code
            code = response.get("code", 0)
            if code != 200:
                logger.error("[%d/%d] SQLFlow returned code %d: %s",
                            i, len(statements), code,
                            response.get("error", "unknown error"))
                errors += 1
                continue

            # Extract lineage
            lineages = extract_lineage(response)
            all_lineages.extend(lineages)

            if lineages:
                for tl in lineages:
                    logger.info("  Lineage: %s --> %s (%d columns)",
                                tl.upstream_table, tl.downstream_table,
                                len(tl.column_mappings))
            else:
                logger.info("  No table-level lineage found (may be DML without persistent targets)")

        except RateLimitError as e:
            logger.error("\n%s", e)
            sys.exit(2)  # special exit code for rate limit

        except SQLFlowError as e:
            logger.error("[%d/%d] SQLFlow error: %s", i, len(statements), e)
            errors += 1

        except Exception as e:
            logger.error("[%d/%d] Unexpected error: %s", i, len(statements), e)
            errors += 1

    # --- Summary ---
    logger.info("--- Summary ---")
    logger.info("Statements processed: %d", len(statements))
    logger.info("Errors: %d", errors)
    logger.info("Table-level lineages found: %d", len(all_lineages))

    if not all_lineages:
        logger.info("No lineage to emit.")
        sys.exit(0 if errors == 0 else 1)

    # --- Build MCPs ---
    mcps = build_mcps(all_lineages, config.datahub.platform, config.datahub.env)

    # --- Emit to DataHub ---
    emitted = emit_to_datahub(mcps, config.datahub, dry_run=args.dry_run)

    if args.dry_run:
        logger.info("[DRY RUN] Would have emitted %d MCPs. "
                    "Remove --dry-run to emit to DataHub at %s",
                    emitted, config.datahub.server)
    else:
        logger.info("Done. Emitted %d MCPs to %s", emitted, config.datahub.server)

    sys.exit(0 if errors == 0 else 1)


if __name__ == "__main__":
    main()
