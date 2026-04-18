"""Tests for lineage_mapper — SQLFlow JSON to DataHub lineage structures."""

from gsp_datahub_sidecar.lineage_mapper import extract_lineage


# Minimal SQLFlow response matching the BigQuery procedural SQL from Issue #11654
BIGQUERY_RESPONSE = {
    "code": 200,
    "data": {
        "sqlflow": {
            "relationships": [
                # Flow 1: project.dataset.view_name -> RS-1 -> temp_table
                {
                    "id": "1", "type": "fdd", "effectType": "select",
                    "target": {"parentName": "RS-1", "column": "IDFIELD"},
                    "sources": [{"parentName": "PROJECT.DATASET.VIEW_NAME", "column": "IDFIELD"}],
                },
                {
                    "id": "2", "type": "fdd", "effectType": "select",
                    "target": {"parentName": "RS-1", "column": "EMAIL"},
                    "sources": [{"parentName": "PROJECT.DATASET.VIEW_NAME", "column": "EMAIL"}],
                },
                {
                    "id": "3", "type": "fdd", "effectType": "create_table",
                    "target": {"parentName": "TEMP_TABLE", "column": "IDFIELD"},
                    "sources": [{"parentName": "RS-1", "column": "IDFIELD"}],
                },
                {
                    "id": "4", "type": "fdd", "effectType": "create_table",
                    "target": {"parentName": "TEMP_TABLE", "column": "EMAIL"},
                    "sources": [{"parentName": "RS-1", "column": "EMAIL"}],
                },
                # Flow 2: temp_table_delta -> RS-4 -> final_output
                {
                    "id": "5", "type": "fdd", "effectType": "select",
                    "target": {"parentName": "RS-4", "column": "USERID"},
                    "sources": [{"parentName": "TEMP_TABLE_DELTA", "column": "USERID"}],
                },
                {
                    "id": "6", "type": "fdd", "effectType": "create_table",
                    "target": {"parentName": "FINAL_OUTPUT", "column": "USERID"},
                    "sources": [{"parentName": "RS-4", "column": "USERID"}],
                },
            ]
        }
    }
}


def test_extracts_two_table_lineages():
    lineages = extract_lineage(BIGQUERY_RESPONSE)
    assert len(lineages) == 2

    upstream_downstream = {(tl.upstream_table, tl.downstream_table) for tl in lineages}
    assert ("PROJECT.DATASET.VIEW_NAME", "TEMP_TABLE") in upstream_downstream
    assert ("TEMP_TABLE_DELTA", "FINAL_OUTPUT") in upstream_downstream


def test_resolves_through_intermediates():
    """RS-1 and RS-4 are intermediate result sets — they should be resolved."""
    lineages = extract_lineage(BIGQUERY_RESPONSE)

    for tl in lineages:
        # No intermediate names in the final output
        assert not tl.upstream_table.startswith("RS-")
        assert not tl.upstream_table.startswith("RESULT_OF_")
        assert not tl.downstream_table.startswith("RS-")


def test_column_mappings():
    lineages = extract_lineage(BIGQUERY_RESPONSE)
    view_to_temp = [tl for tl in lineages
                    if tl.upstream_table == "PROJECT.DATASET.VIEW_NAME"][0]

    columns = set(view_to_temp.column_mappings)
    assert ("IDFIELD", "IDFIELD") in columns
    assert ("EMAIL", "EMAIL") in columns


def test_empty_response():
    lineages = extract_lineage({"code": 200, "data": {}})
    assert lineages == []


def test_no_persistent_effects():
    """If all relationships are SELECT (no CREATE/INSERT), no table lineage is emitted."""
    response = {
        "data": {
            "sqlflow": {
                "relationships": [
                    {
                        "id": "1", "type": "fdd", "effectType": "select",
                        "target": {"parentName": "RS-1", "column": "A"},
                        "sources": [{"parentName": "TABLE1", "column": "A"}],
                    },
                ]
            }
        }
    }
    lineages = extract_lineage(response)
    assert lineages == []


def test_resolves_through_function_nodes():
    """Function nodes (ARRAY_AGG, etc.) should be treated as intermediates.

    Reproduces the pattern from DataHub #11670: BigQuery deduplication macro
    using ARRAY_AGG(...)[OFFSET(0)]. The lineage chain is:
      ALL_ARTICLES.ORIGINAL -> ARRAY_AGG.ARRAY_AGG -> RS-1.UNIQUE
        -> RS-2.UNIQUE -> DEDUPLICATED_ARTICLES.UNIQUE
    The mapper must resolve through both RS-* and ARRAY_AGG to reach ALL_ARTICLES.
    """
    response = {
        "data": {
            "sqlflow": {
                "dbobjs": {
                    "servers": [{
                        "name": "DEFAULT_SERVER",
                        "dbVendor": "dbvbigquery",
                        "databases": [{
                            "name": "DEFAULT",
                            "schemas": [{
                                "name": "DEFAULT",
                                "tables": [{
                                    "id": "11", "name": "all_articles", "type": "table",
                                    "columns": [
                                        {"id": "12", "name": "original"},
                                        {"id": "13", "name": "id"},
                                    ],
                                }],
                                "others": [
                                    {"id": "7", "name": "RS-1", "type": "select_list"},
                                    {"id": "19", "name": "RS-2", "type": "select_list"},
                                    {"id": "16", "name": "array_agg", "type": "function"},
                                ],
                            }],
                        }],
                    }],
                },
                "relationships": [
                    # ALL_ARTICLES.ORIGINAL -> ARRAY_AGG (function)
                    {
                        "id": "2", "type": "fdd", "effectType": "function",
                        "target": {"parentName": "ARRAY_AGG", "column": "ARRAY_AGG"},
                        "sources": [{"parentName": "ALL_ARTICLES", "column": "ORIGINAL"}],
                    },
                    # ARRAY_AGG -> RS-1.UNIQUE (select)
                    {
                        "id": "1", "type": "fdd", "effectType": "select",
                        "target": {"parentName": "RS-1", "column": "UNIQUE"},
                        "sources": [{"parentName": "ARRAY_AGG", "column": "ARRAY_AGG"}],
                    },
                    # RS-1.UNIQUE -> RS-2.UNIQUE (select — outer query)
                    {
                        "id": "4_0", "type": "fdd", "effectType": "select",
                        "target": {"parentName": "RS-2", "column": "UNIQUE"},
                        "sources": [{"parentName": "RS-1", "column": "UNIQUE"}],
                    },
                    # RS-2.UNIQUE -> DEDUPLICATED_ARTICLES.UNIQUE (create_table)
                    {
                        "id": "5_0", "type": "fdd", "effectType": "create_table",
                        "target": {"parentName": "DEDUPLICATED_ARTICLES", "column": "UNIQUE"},
                        "sources": [{"parentName": "RS-2", "column": "UNIQUE"}],
                        "processId": "5",
                    },
                ],
            }
        }
    }
    lineages = extract_lineage(response)
    assert len(lineages) == 1

    tl = lineages[0]
    assert tl.upstream_table == "ALL_ARTICLES"
    assert tl.downstream_table == "DEDUPLICATED_ARTICLES"
    assert ("ORIGINAL", "UNIQUE") in tl.column_mappings


def test_skips_self_references():
    response = {
        "data": {
            "sqlflow": {
                "relationships": [
                    {
                        "id": "1", "type": "fdd", "effectType": "insert",
                        "target": {"parentName": "TABLE1", "column": "A"},
                        "sources": [{"parentName": "TABLE1", "column": "A"}],
                    },
                ]
            }
        }
    }
    lineages = extract_lineage(response)
    assert lineages == []
