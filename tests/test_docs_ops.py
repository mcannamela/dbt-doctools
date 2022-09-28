from networkx import DiGraph

from dbt_doctools.docs_ops import consolidate_duplicate_docs_blocks_, compute_doc_depth


def test_consolidate_duplicate_docs_blocks_(manifest, graph, config):
    consolidate_duplicate_docs_blocks_(manifest, graph, config)


def test_compute_doc_depth():
    g = DiGraph()
    edges = [
        ('a', 'b1'),
        ('a', 'b2'),
        ('b1', 'c1'),
        ('c1', 'd1'),
        ('b2', 'd1'),
        ('b2', 'c2'),
        ('c2', 'd2'),

    ]

    blocks = {
        'a':['ka'],
        'b1':['ka', 'kb'],
        'b2':['kb'],
        'c1':['kc', 'kb'],
        'c2':[],
        'd1':['kd1'],
        'd2':['kd2', 'kc'],
    }

    def get_blocks_iter(n):
        return blocks[n]

    exp_block_depth = {
        'ka': 0,
        'kb': 1,
        'kc': 2,
        'kd1': 2,
        'kd2': 3,
    }

    g.add_edges_from(edges)

    block_depth = compute_doc_depth(g, get_blocks_iter)

    assert block_depth == exp_block_depth