from typing import TypeVar, List, Callable

from loguru import logger
from networkx import DiGraph

N = TypeVar('N')
S = TypeVar('S')


def propagate_breadth_first(
        g:DiGraph,
        source_nodes:List[N],
        state:S,
        propagate_it:Callable[[N, N, S ], S]= lambda source, target, state: logger.info(f"Propagate from {source} to {target} with state {state}")
)->S:
    if not source_nodes:
        return state

    next_successors = []
    for s in source_nodes:
        successors = list(g.successors(s))
        next_successors.append(successors)
        for n in successors:
            state = propagate_it(s, n, state)

    for succ in next_successors:
        state = propagate_breadth_first(g, succ, state, propagate_it)

    return state
