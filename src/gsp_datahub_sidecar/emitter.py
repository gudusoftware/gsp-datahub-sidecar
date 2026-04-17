"""Emit lineage to DataHub as Metadata Change Proposals (MCPs)."""

import logging

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper

from .config import DataHubConfig
from .lineage_mapper import TableLineage

logger = logging.getLogger(__name__)


def _make_dataset_urn(table_name: str, platform: str, env: str) -> str:
    """Convert a SQLFlow table name to a DataHub dataset URN.

    SQLFlow returns names like:
      - "PROJECT.DATASET.TABLE"  (BigQuery 3-part)
      - "SCHEMA.TABLE"           (2-part)
      - "TABLE"                  (1-part, uses DEFAULT schema)

    DataHub URN format:
      urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})
    """
    # Normalize: lowercase, replace spaces
    name = table_name.strip().lower()

    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})"


def _make_field_urn(dataset_urn: str, column_name: str) -> str:
    """Create a DataHub schema field URN for column-level lineage."""
    col = column_name.strip().lower()
    # Strip quotes
    col = col.strip('"').strip("'").strip("`")
    return f"urn:li:schemaField:({dataset_urn},{col})"


def build_mcps(
    lineages: list[TableLineage],
    platform: str,
    env: str,
    column_lineage: bool = True,
) -> list[MetadataChangeProposalWrapper]:
    """Convert TableLineage objects to DataHub MetadataChangeProposalWrappers.

    Groups all upstreams for the same downstream table into a single
    UpstreamLineage aspect (DataHub expects one aspect per entity).

    When ``column_lineage`` is False, emits table-level upstreams only and
    skips the ``fineGrainedLineages`` field.
    """
    # Group by downstream table
    downstream_map: dict[str, list[TableLineage]] = {}
    for tl in lineages:
        key = tl.downstream_table
        downstream_map.setdefault(key, []).append(tl)

    mcps = []
    total_column_mappings = 0

    for downstream_table, tl_list in downstream_map.items():
        downstream_urn = _make_dataset_urn(downstream_table, platform, env)
        upstreams = []
        fine_grained = []

        for tl in tl_list:
            upstream_urn = _make_dataset_urn(tl.upstream_table, platform, env)

            # Table-level upstream
            upstreams.append(UpstreamClass(
                dataset=upstream_urn,
                type=DatasetLineageTypeClass.TRANSFORMED,
            ))

            if not column_lineage:
                continue

            # Column-level (fine-grained) lineage
            for src_col, tgt_col in tl.column_mappings:
                # Skip wildcard columns
                if src_col == "*" or tgt_col == "*":
                    continue

                upstream_field = _make_field_urn(upstream_urn, src_col)
                downstream_field = _make_field_urn(downstream_urn, tgt_col)

                fine_grained.append(FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=[upstream_field],
                    downstreams=[downstream_field],
                ))

        lineage_aspect = UpstreamLineageClass(
            upstreams=upstreams,
            fineGrainedLineages=fine_grained if fine_grained else None,
        )

        mcp = MetadataChangeProposalWrapper(
            entityUrn=downstream_urn,
            aspect=lineage_aspect,
        )
        mcps.append(mcp)
        total_column_mappings += len(fine_grained)
        logger.debug("Built MCP for %s with %d upstreams, %d column mappings",
                      downstream_urn, len(upstreams), len(fine_grained))

    if column_lineage:
        logger.info("Built %d MCPs for %d downstream tables (%d column-level mappings)",
                    len(mcps), len(downstream_map), total_column_mappings)
    else:
        logger.info("Built %d MCPs for %d downstream tables (table-level only — column lineage disabled)",
                    len(mcps), len(downstream_map))
    return mcps


def emit_to_datahub(
    mcps: list[MetadataChangeProposalWrapper],
    config: DataHubConfig,
    dry_run: bool = False,
) -> int:
    """Emit MCPs to DataHub GMS. Returns the number of MCPs emitted.

    If dry_run is True, logs what would be emitted but doesn't send.
    """
    if dry_run:
        logger.info("[DRY RUN] Would emit %d MCPs to %s", len(mcps), config.server)
        for mcp in mcps:
            aspect = mcp.aspect
            ups = len(aspect.upstreams) if aspect and aspect.upstreams else 0
            fg = aspect.fineGrainedLineages or [] if aspect else []
            logger.info("[DRY RUN]   %s  (%d upstream table(s), %d column-level lineage(s))",
                        mcp.entityUrn, ups, len(fg))
            # Show up to 5 column mappings for visual confirmation.
            for fgl in fg[:5]:
                up = fgl.upstreams[0] if fgl.upstreams else "?"
                down = fgl.downstreams[0] if fgl.downstreams else "?"
                logger.info("[DRY RUN]     %s -> %s", up, down)
            if len(fg) > 5:
                logger.info("[DRY RUN]     ... and %d more column mapping(s)", len(fg) - 5)
        return len(mcps)

    emitter = DatahubRestEmitter(
        gms_server=config.server,
        token=config.token,
    )

    emitted = 0
    for mcp in mcps:
        try:
            emitter.emit(mcp)
            emitted += 1
            logger.info("Emitted lineage for %s", mcp.entityUrn)
        except Exception as e:
            logger.error("Failed to emit MCP for %s: %s", mcp.entityUrn, e)

    logger.info("Successfully emitted %d / %d MCPs to %s", emitted, len(mcps), config.server)
    return emitted
