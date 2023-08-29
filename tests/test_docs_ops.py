from dbt.contracts.graph.parsed import ParsedDocumentation
from networkx import DiGraph

from dbt_doctools.dbt_api import obtain_manifest_and_graph_and_config
from dbt_doctools.docs_ops import consolidate_duplicate_docs_blocks_, compute_min_doc_depth, make_doc_sort_fun, \
    is_non_empty, find_consolidated_docs_and_duplicates, find_degenerate_docs, propagate_column_descriptions_
from test_support.dbt_fixtures import dbt_dummy_project_path


def test_propagate_column_descriptions_(manifest, graph, config):
    propagate_column_descriptions_(manifest, graph, config)


def test_consolidate_duplicate_docs_blocks_(manifest, graph, config):
    def non_empty_docs(m):
        return {k: d for k, d in m.docs.items() if is_non_empty(d)}

    degenerate_docs = find_degenerate_docs(non_empty_docs(manifest))

    consolidate_duplicate_docs_blocks_(manifest, graph, config)

    manifest_without_dupes, _, _ = obtain_manifest_and_graph_and_config(
        dbt_command_args=['--project-dir', str(dbt_dummy_project_path())]
    )

    lack_of_degenerate_docs = find_degenerate_docs(non_empty_docs(manifest_without_dupes))

    assert len(degenerate_docs) > 0
    assert len(lack_of_degenerate_docs) == 0


def test_find_consolidated_docs_and_duplicates(config, graph, manifest):
    consolidated_docs_and_duplicates = find_consolidated_docs_and_duplicates(manifest, graph, config)
    for doc, dupes in consolidated_docs_and_duplicates:
        for d in dupes:
            assert doc.block_contents.strip() == d.block_contents.strip()

    doc_id_to_dupe_ids = {t[0].unique_id: [u.unique_id for u in t[1]] for t in consolidated_docs_and_duplicates}
    project = config.project_name
    assert doc_id_to_dupe_ids[f'{project}.zero_layer__old_value'] == [f'{project}.one_layer__old_value']


def test_is_non_empty():
    empty = ParsedDocumentation(
        package_name='blah',
        root_path='blah',
        path='blah',
        original_file_path='blah',
        name='blah',
        unique_id='blah',
        block_contents=''
    )

    non_empty = ParsedDocumentation(
        package_name='blah',
        root_path='blah',
        path='blah',
        original_file_path='blah',
        name='blah',
        unique_id='blah',
        block_contents='some doc words'
    )

    assert not is_non_empty(empty)
    assert is_non_empty(non_empty)


def test_make_doc_sort_fun(manifest, graph, config):
    sort_key = make_doc_sort_fun(manifest, graph, config)

    sorted_docs = list(sorted(manifest.docs.keys(), key=sort_key))
    project = config.project_name
    assert sorted_docs.index(f'{project}.some_old_source__value') < sorted_docs.index(
        f'{project}.zero_layer__old_value')
    assert sorted_docs.index(f'{project}.static_map_value') < sorted_docs.index(f'{project}.zero_layer__old_value')
    assert sorted_docs.index(f'{project}.zero_layer__old_value') < sorted_docs.index(f'{project}.one_layer__old_value')


def test_compute_min_doc_depth():
    g = DiGraph()
    edges = [
        ('a', 'b1'),
        ('a', 'b2'),
        ('b1', 'c1'),
        ('c1', 'd1'),
        ('b2', 'd1'),
        ('b2', 'c2'),
        ('c2', 'd2'),
        ('d2', 'e')

    ]

    blocks = {
        'a': ['ka'],
        'b1': ['ka', 'kb'],
        'b2': ['kb'],
        'c1': ['kc', 'kb'],
        'c2': [],
        'd1': ['kd1'],
        'd2': ['kd2', 'kc'],
        'e': ['ke'],

    }

    def get_blocks_iter(n):
        return blocks[n]

    exp_block_depth = {
        'ka': 0,
        'kb': 1,
        'kc': 2,
        'kd1': 2,
        'kd2': 3,
        'ke': 4
    }

    g.add_edges_from(edges)

    block_depth = compute_min_doc_depth(g, get_blocks_iter)

    assert block_depth == exp_block_depth
