"""Emit lineage to DataHub as Metadata Change Proposals (MCPs)."""

import logging

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetKeyClass,
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
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
    # Track all dataset URNs so we can ensure they exist in DataHub
    all_dataset_urns: dict[str, str] = {}  # urn -> table_name
    # Track columns per dataset URN for schema metadata
    dataset_columns: dict[str, set[str]] = {}  # urn -> set of column names

    for downstream_table, tl_list in downstream_map.items():
        downstream_urn = _make_dataset_urn(downstream_table, platform, env)
        all_dataset_urns[downstream_urn] = downstream_table
        upstreams = []
        fine_grained = []

        for tl in tl_list:
            upstream_urn = _make_dataset_urn(tl.upstream_table, platform, env)
            all_dataset_urns[upstream_urn] = tl.upstream_table

            # Table-level upstream
            upstreams.append(UpstreamClass(
                dataset=upstream_urn,
                type=DatasetLineageTypeClass.TRANSFORMED,
            ))

            if not column_lineage:
                continue

            # Column-level (fine-grained) lineage
            for src_col, tgt_col in tl.column_mappings:
                # Collect columns for schema metadata (skip wildcards —
                # "*" is not a real column name for schema registration)
                src_col_clean = src_col.strip().lower().strip('"').strip("'").strip("`")
                tgt_col_clean = tgt_col.strip().lower().strip('"').strip("'").strip("`")
                if src_col_clean != "*":
                    dataset_columns.setdefault(upstream_urn, set()).add(src_col_clean)
                if tgt_col_clean != "*":
                    dataset_columns.setdefault(downstream_urn, set()).add(tgt_col_clean)

                upstream_field = _make_field_urn(upstream_urn, src_col)
                downstream_field = _make_field_urn(downstream_urn, tgt_col)

                fine_grained.append(FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=[upstream_field],
                    downstreams=[downstream_field],
                    confidenceScore=1.0,
                    transformOperation="IDENTITY",
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

    # Emit datasetKey aspects for all referenced entities so DataHub
    # creates them and the lineage graph renders upstream nodes.
    env_fabric = env.upper()
    key_mcps = []
    for urn, table_name in all_dataset_urns.items():
        name = table_name.strip().lower()
        key_mcp = MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DatasetKeyClass(
                platform=f"urn:li:dataPlatform:{platform}",
                name=name,
                origin=env_fabric,
            ),
        )
        key_mcps.append(key_mcp)

    # Emit schemaMetadata aspects so DataHub can render column-level lineage.
    # Without schema fields registered, the UI won't draw column arrows.
    schema_mcps = []
    if column_lineage:
        for urn, columns in dataset_columns.items():
            table_name = all_dataset_urns.get(urn, "unknown")
            fields = [
                SchemaFieldClass(
                    fieldPath=col,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="string",
                )
                for col in sorted(columns)
            ]
            schema_mcp = MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=SchemaMetadataClass(
                    schemaName=table_name.strip().lower(),
                    platform=f"urn:li:dataPlatform:{platform}",
                    version=0,
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                    fields=fields,
                ),
            )
            schema_mcps.append(schema_mcp)

    # Order: entity keys first, then schemas, then lineage
    mcps = key_mcps + schema_mcps + mcps

    if column_lineage:
        logger.info("Built %d MCPs for %d downstream tables (%d column-level mappings)",
                    len(mcps) - len(key_mcps), len(downstream_map), total_column_mappings)
    else:
        logger.info("Built %d MCPs for %d downstream tables (table-level only — column lineage disabled)",
                    len(mcps) - len(key_mcps), len(downstream_map))
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
            aspect_name = type(aspect).__name__ if aspect else "None"

            if isinstance(aspect, UpstreamLineageClass):
                ups = len(aspect.upstreams) if aspect.upstreams else 0
                fg = aspect.fineGrainedLineages or []
                logger.info("[DRY RUN]   %s  [%s] (%d upstream table(s), %d column-level lineage(s))",
                            mcp.entityUrn, aspect_name, ups, len(fg))
                for fgl in fg[:5]:
                    up = fgl.upstreams[0] if fgl.upstreams else "?"
                    down = fgl.downstreams[0] if fgl.downstreams else "?"
                    logger.info("[DRY RUN]     %s -> %s", up, down)
                if len(fg) > 5:
                    logger.info("[DRY RUN]     ... and %d more column mapping(s)", len(fg) - 5)
            elif isinstance(aspect, SchemaMetadataClass):
                n_fields = len(aspect.fields) if aspect.fields else 0
                logger.info("[DRY RUN]   %s  [%s] (%d field(s))",
                            mcp.entityUrn, aspect_name, n_fields)
            else:
                logger.info("[DRY RUN]   %s  [%s]", mcp.entityUrn, aspect_name)
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
