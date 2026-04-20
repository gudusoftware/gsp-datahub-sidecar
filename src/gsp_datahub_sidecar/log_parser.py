"""Parse DataHub ingestion logs to extract SQL statements that sqlglot failed on."""

import logging
import re
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

# DataHub/sqlglot logs this when it can't parse a statement:
#   WARNING {sqlglot.parser:1432} - '...' contains unsupported syntax.
#     Falling back to parsing as a 'Command'
COMMAND_FALLBACK_PATTERN = re.compile(
    r"'(.+?)'\s+contains unsupported syntax\.\s+Falling back to parsing as a 'Command'",
    re.DOTALL,
)

# Also catch direct parse failures:
#   Failed to parse SQL: ...
PARSE_FAILURE_PATTERN = re.compile(
    r"Failed to parse SQL:\s+(.+)",
)


# Power BI M-language escape sequences that must be decoded before SQL parsing.
# M encodes newlines as #(lf), carriage returns as #(cr), and tabs as #(tab).
# Without decoding, -- comments swallow the rest of the query because #(lf) is
# not a real newline.  See https://github.com/datahub-project/datahub/issues/11251
_M_LANGUAGE_ESCAPES = {
    "#(lf)": "\n",
    "#(cr)": "\r",
    "#(cr,lf)": "\r\n",
    "#(tab)": "\t",
}

_M_ESCAPE_PATTERN = re.compile(
    r"#\((?:lf|cr|cr,lf|tab)\)", re.IGNORECASE
)


def normalize_sql(sql: str) -> str:
    """Decode Power BI M-language escape sequences in SQL text.

    Replaces #(lf), #(cr), #(cr,lf), and #(tab) with their real characters
    so that SQL parsers can correctly handle single-line comments (--).
    """
    if "#(" not in sql:
        return sql
    return _M_ESCAPE_PATTERN.sub(
        lambda m: _M_LANGUAGE_ESCAPES[m.group(0).lower()], sql
    )


@dataclass
class FailedStatement:
    """A SQL statement that sqlglot failed to parse."""
    sql: str
    source: str  # where it came from (log file path + line, or "direct input")
    error: str   # the original error message


def parse_log_file(log_path: str) -> list[FailedStatement]:
    """Extract SQL statements that sqlglot failed to parse from DataHub ingestion logs.

    Scans for 'contains unsupported syntax. Falling back to parsing as a Command'
    warnings, which indicate procedural SQL that sqlglot couldn't handle.
    """
    path = Path(log_path)
    if not path.exists():
        raise FileNotFoundError(f"Log file not found: {log_path}")

    content = path.read_text(encoding="utf-8", errors="replace")
    statements = []

    for match in COMMAND_FALLBACK_PATTERN.finditer(content):
        sql_fragment = normalize_sql(match.group(1).strip())
        if sql_fragment:
            statements.append(FailedStatement(
                sql=sql_fragment,
                source=f"{log_path}",
                error="sqlglot: Command fallback",
            ))

    for match in PARSE_FAILURE_PATTERN.finditer(content):
        sql_fragment = normalize_sql(match.group(1).strip())
        if sql_fragment:
            statements.append(FailedStatement(
                sql=sql_fragment,
                source=f"{log_path}",
                error="sqlglot: ParseError",
            ))

    # Deduplicate by SQL text
    seen = set()
    unique = []
    for stmt in statements:
        if stmt.sql not in seen:
            seen.add(stmt.sql)
            unique.append(stmt)

    logger.info("Found %d unique failed statements in %s (from %d total matches)",
                len(unique), log_path, len(statements))
    return unique


# Keywords that indicate procedural SQL — the file should be sent as one block
_PROCEDURAL_KEYWORDS = re.compile(
    r'\b(DECLARE|BEGIN|IF\s+.+\s+THEN|END\s+IF|CALL|EXCEPTION\s+WHEN|LOOP|END\s+LOOP|WHILE)\b',
    re.IGNORECASE,
)


def parse_sql_file(sql_path: str) -> list[FailedStatement]:
    """Read a SQL file and return statements for analysis.

    If the file contains procedural keywords (DECLARE, IF/THEN, CALL, BEGIN/END),
    the entire file is sent as a single statement — splitting on semicolons would
    break the procedural block. Otherwise, splits on semicolons.
    """
    path = Path(sql_path)
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    content = path.read_text(encoding="utf-8")

    content = normalize_sql(content)

    # Procedural SQL: send as one block (semicolons are inside the block)
    if _PROCEDURAL_KEYWORDS.search(content):
        logger.info("Detected procedural SQL in %s — sending as single statement", sql_path)
        return [FailedStatement(
            sql=content.strip(),
            source=sql_path,
            error="direct input",
        )]

    # Non-procedural: split on semicolons
    raw_stmts = [s.strip() for s in content.split(";") if s.strip()]

    if len(raw_stmts) <= 1:
        return [FailedStatement(
            sql=content.strip(),
            source=sql_path,
            error="direct input",
        )]

    return [
        FailedStatement(sql=s, source=sql_path, error="direct input")
        for s in raw_stmts
    ]


def parse_sql_text(sql_text: str) -> list[FailedStatement]:
    """Wrap a direct SQL string as a FailedStatement."""
    return [FailedStatement(
        sql=normalize_sql(sql_text.strip()),
        source="cli input",
        error="direct input",
    )]
