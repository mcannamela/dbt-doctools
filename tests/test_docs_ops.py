from dbt_doctools.docs_ops import consolidate_duplicate_docs_blocks_


def test_consolidate_duplicate_docs_blocks_(manifest, graph, config):
    consolidate_duplicate_docs_blocks_(manifest, graph, config)
