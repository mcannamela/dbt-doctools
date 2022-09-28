import itertools
from collections import defaultdict
from typing import Iterable, List, Dict, Tuple, Callable, MutableMapping

from dbt.config import RuntimeConfig
from dbt.contracts.files import SchemaSourceFile, AnySourceFile
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedDocumentation
from dbt.graph import Graph
from networkx import DiGraph

from dbt_doctools.manifest_tools import build_docs_block_to_ref_map, ref_id_and_column_extractor, \
    source_id_and_column_extractor, iter_node_or_sources_files, IdSetMap
from dbt_doctools.markdown_ops import DocsBlock


def is_non_empty(doc: ParsedDocumentation):
    return len(doc.block_contents.strip()) > 0


def consolidate_duplicate_docs_blocks_(manifest: Manifest, graph: Graph, config: RuntimeConfig):
    sort_key = make_doc_sort_fun(manifest, graph, config)

    non_empty_docs = {k: d for k, d in manifest.docs.items() if is_non_empty(d)}
    block_content_to_docs: MutableMapping[str, List[ParsedDocumentation]] = defaultdict(list)
    for d in non_empty_docs.values():
        block_content_to_docs[d.block_contents.strip()].append(d)

    degenerate_docs: List[List[ParsedDocumentation]] = [
        list(sorted(v, key=lambda d: sort_key(d.unique_id)))[1:]
        for v in block_content_to_docs.values() if len(v) > 1
    ]

    consolidated_docs_and_duplicates = [(x[0], x[1:]) for x in degenerate_docs]

    duplicate_docs_to_remove: List[ParsedDocumentation] = sum([t[1] for t in consolidated_docs_and_duplicates], [])

    doc_file_to_docs, doc_files_to_rewrite = _construct_docs_to_rewrite(duplicate_docs_to_remove, manifest)

    block_id_to_model_ids, ref_id_to_block_ids, block_id_to_file_ids = build_docs_block_to_ref_map(
        ref_id_and_column_extractor(manifest, config.project_name),
        source_id_and_column_extractor(manifest, config.project_name),
        iter_node_or_sources_files(manifest)
    )

    return doc_files_to_rewrite, doc_file_to_docs


def _construct_docs_to_rewrite(duplicate_docs_to_remove: List[ParsedDocumentation], manifest: Manifest) -> Tuple[
    MutableMapping[str, List[ParsedDocumentation]], Dict[str, AnySourceFile]]:
    doc_files_to_rewrite: Dict[str, AnySourceFile] = {d.file_id: manifest.files[d.file_id] for d in
                                                      duplicate_docs_to_remove}
    duplicate_doc_ids = {d.unique_id for d in duplicate_docs_to_remove}
    doc_file_to_docs: MutableMapping[str, List[ParsedDocumentation]] = defaultdict(list)
    for d in manifest.docs.values():
        if d.file_id in doc_files_to_rewrite and d.unique_id not in duplicate_doc_ids:
            doc_file_to_docs[d.file_id].append(d)
    return doc_file_to_docs, doc_files_to_rewrite


def make_doc_sort_fun(manifest: Manifest, graph: Graph, config: RuntimeConfig):
    _, ref_id_to_block_ids, _ = build_docs_block_to_ref_map(
        ref_id_and_column_extractor(manifest, config.project_name),
        source_id_and_column_extractor(manifest, config.project_name),
        iter_node_or_sources_files(manifest)
    )

    def get_blocks_iter(n):
        return ref_id_to_block_ids.get(n, [])

    g:DiGraph = graph.graph
    doc_depth = compute_doc_depth(g, get_blocks_iter)

    def sort_key(doc_name):
        return (doc_depth.get(doc_name, 2 ** 31), doc_name)

    return sort_key


def compute_doc_depth(g:DiGraph, get_blocks_iter: Callable[[str], Iterable[str]]):
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
