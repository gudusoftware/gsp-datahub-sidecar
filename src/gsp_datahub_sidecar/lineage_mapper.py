"""Map SQLFlow lineage JSON to DataHub Metadata Change Proposals (MCPs)."""

import logging
from collections import defaultdict
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# effectTypes that represent data movement between persistent objects
PERSISTENT_EFFECT_TYPES = {"create_view", "create_table", "insert", "merge", "ctas", "update"}

# Prefixes for intermediate result sets (not real tables)
INTERMEDIATE_PREFIXES = ("RS-", "RESULT_OF_")


@dataclass
class ColumnLineage:
    """A single column-level lineage relationship."""
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    effect_type: str


@dataclass
class TableLineage:
    """Table-level lineage with column details."""
    upstream_table: str
    downstream_table: str
    column_mappings: list[tuple[str, str]] = field(default_factory=list)
    # (source_column, target_column) pairs


def _is_intermediate(name: str) -> bool:
    """Check if a name refers to an intermediate result set rather than a real table."""
    upper = name.upper()
    return any(upper.startswith(p) for p in INTERMEDIATE_PREFIXES)


def _find_key(obj, key: str):
    """Recursively search a nested dict for a key. Returns the first match."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            result = _find_key(v, key)
            if result is not None:
                return result
    return None


def extract_lineage(sqlflow_response: dict) -> list[TableLineage]:
    """Extract table-level lineage (with column mappings) from SQLFlow API response.

    Walks the 'relationships' array in the SQLFlow JSON. For relationships
    involving persistent objects (CREATE VIEW, INSERT, etc.), maps source
    tables to target tables. Intermediate result sets (RS-*, RESULT_OF_*)
    are traversed to find the real source tables.

    Returns a list of TableLineage objects suitable for DataHub MCP emission.
    """
    relationships = _find_key(sqlflow_response, "relationships")
    if not relationships:
        logger.warning("No 'relationships' found in SQLFlow response")
        return []

    # Phase 1: collect all fdd relationships
    all_rels = [r for r in relationships if r.get("type") == "fdd"]
    logger.debug("Total fdd relationships: %d", len(all_rels))

    # Phase 2: build a reverse lookup — for each intermediate column,
    # trace back to the real source columns
    # Key: (parentName, column) -> list of (sourceParentName, sourceColumn)
    reverse_map: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    for rel in all_rels:
        tgt = rel["target"]
        tgt_key = (tgt["parentName"], tgt["column"])
        for src in rel.get("sources", []):
            reverse_map[tgt_key].append((src["parentName"], src["column"]))

    def resolve_sources(parent_name: str, column: str, visited: set | None = None) -> list[tuple[str, str]]:
        """Recursively resolve intermediate result sets to real source tables."""
        if visited is None:
            visited = set()

        key = (parent_name, column)
        if key in visited:
            return []  # cycle protection
        visited.add(key)

        if not _is_intermediate(parent_name):
            return [(parent_name, column)]

        # It's an intermediate — look up what feeds into it
        sources = reverse_map.get(key, [])
        if not sources:
            return [(parent_name, column)]  # can't resolve further

        real_sources = []
        for src_parent, src_col in sources:
            real_sources.extend(resolve_sources(src_parent, src_col, visited))
        return real_sources

    # Phase 3: for each "persistent effect" relationship, resolve sources
    # and build table-level lineage
    table_lineage_map: dict[tuple[str, str], TableLineage] = {}

    for rel in all_rels:
        effect = rel.get("effectType", "")
        if effect not in PERSISTENT_EFFECT_TYPES:
            continue

        target = rel["target"]
        target_table = target["parentName"]
        target_column = target["column"]

        if _is_intermediate(target_table):
            continue  # target should be a real table

        for src in rel.get("sources", []):
            src_parent = src["parentName"]
            src_column = src["column"]

            # Resolve through intermediates
            real_sources = resolve_sources(src_parent, src_column)

            for real_table, real_column in real_sources:
                if _is_intermediate(real_table):
                    continue
                if real_table == target_table:
                    continue  # skip self-references

                pair_key = (real_table, target_table)
                if pair_key not in table_lineage_map:
                    table_lineage_map[pair_key] = TableLineage(
                        upstream_table=real_table,
                        downstream_table=target_table,
                    )
                table_lineage_map[pair_key].column_mappings.append(
                    (real_column, target_column)
                )

    lineages = list(table_lineage_map.values())

    # Deduplicate column mappings within each table lineage
    for tl in lineages:
        tl.column_mappings = list(set(tl.column_mappings))

    logger.info("Extracted %d table-level lineage relationships", len(lineages))
    for tl in lineages:
        logger.info("  %s --> %s (%d columns)",
                     tl.upstream_table, tl.downstream_table, len(tl.column_mappings))

    return lineages
