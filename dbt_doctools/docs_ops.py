import functools
import itertools
import operator
from typing import Iterable, List, Dict, Tuple, Callable

from dbt.config import read_user_config, RuntimeConfig
from dbt.contracts.files import SchemaSourceFile
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedDocumentation
from dbt.graph import Graph
from networkx import DiGraph
from collections import defaultdict

from dbt_doctools.markdown_ops import DocsBlock


def is_non_empty(doc:ParsedDocumentation):
    return len(doc.block_contents.strip())>0


def consolidate_duplicate_docs_blocks_(manifest:Manifest, graph:Graph, config:RuntimeConfig):
    sort_key = make_doc_sort_fun(manifest, graph, config)

    g:DiGraph = graph.graph
    non_empty_docs = {k:d for k,d in manifest.docs.items() if is_non_empty(d)}
    block_content_to_docs = defaultdict(list)
    for d in non_empty_docs.values():
        block_content_to_docs[d.block_contents.strip()].append(d)

    duplicate_docs_to_remove = sum(
        [list(sorted(v, key=lambda d: sort_key(d.unique_id)))[1:] for v in block_content_to_docs.values() if len(v) > 1]
        ,[]
    )

    files_to_rewrite = {d.file_id for d in duplicate_docs_to_remove}

    return duplicate_docs_to_remove



def make_doc_sort_fun(manifest:Manifest, graph:Graph, config:RuntimeConfig):
    _, ref_to_docs = build_docs_block_to_ref_map(manifest, config.project_name)
    _, source_to_docs = build_docs_block_to_source_map(manifest, config.project_name)

    def get_blocks_iter(n):
        return itertools.chain(source_to_docs.get(n, []), ref_to_docs.get(n, []))

    doc_depth = compute_doc_depth(graph.graph, get_blocks_iter)

    def sort_key(doc_name):
        return (doc_depth.get(doc_name, 2 ** 31), doc_name)

    return sort_key


def compute_doc_depth(g, get_blocks_iter:Callable[[str], Iterable[str]]):
    doc_depth = {}



    node_set = {n for n in g if g.in_degree(n) == 0}
    depth = 0
    while node_set:
        for n in node_set:
            for b in get_blocks_iter(n):
                doc_depth[b] = depth
        node_set = set(itertools.chain(*[g.successors(n) for n in node_set]))
        depth += 1
    return doc_depth


def build_docs_block_to_ref_map(manifest, project_name:str)->Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    block_to_models = defaultdict(list)
    model_to_blocks = defaultdict(list)

    for fid, f in manifest.files.items():
        if isinstance(f, SchemaSourceFile) and f.node_patches:
            for ref_dfy in itertools.chain(f.dict_from_yaml.get('models', []), f.dict_from_yaml.get('seeds', [])):
                ref_id = manifest.ref_lookup.get_unique_id(ref_dfy['name'], project_name)
                for column_dfy in ref_dfy.get('columns', []):
                    referenced_block_names = DocsBlock.referenced_doc_names(column_dfy.get('description', ''))
                    for b in referenced_block_names:
                        block_to_models[b].append(ref_id)
                        model_to_blocks[ref_id].append(b)

    return block_to_models, model_to_blocks


def build_docs_block_to_source_map(manifest, project_name:str)->Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    block_to_sources = defaultdict(list)
    source_to_blocks = defaultdict(list)

    for fid, f in manifest.files.items():
        if isinstance(f, SchemaSourceFile) and f.sources:
            for source_dfy in f.dict_from_yaml['sources']:
                for table_dfy in source_dfy.get('tables', []):
                    source_id = manifest.resolve_source(source_dfy['name'], table_dfy['name'], project_name, None).unique_id
                    for column_dfy in table_dfy.get('columns', []):
                        referenced_block_names = DocsBlock.referenced_doc_names(column_dfy.get('description', ''))
                        for b in referenced_block_names:
                            block_to_sources[b].append(source_id)
                            source_to_blocks[source_id].append(b)

    return block_to_sources, source_to_blocks