from networkx import DiGraph

from dbt_doctools.graph_ops import propagate_breadth_first


def test_propagate_breadth_first():
    g = DiGraph()
    g.add_edge(0, 1)
    g.add_edge(1, 2)
    g.add_edge(2, 3)

    g.add_edge(10, 2)

    state = {n: {n} for n in g}

    exp_state = {
        0:{0},
        10: {10},
        1: {0,1},
        2: {0,1,2, 10},
        3: {0,1,2, 10, 3},
    }

    def propagate_it(a,b,s):
        s[b] = s[b] | s[a]
        s[b] = s[b] | s[a]
        return s

    propagated_state = propagate_breadth_first(g,[0, 10], state, propagate_it)

    assert propagated_state == exp_state
