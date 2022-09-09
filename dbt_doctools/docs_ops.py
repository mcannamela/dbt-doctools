import functools
import itertools
import operator
from typing import Iterable, List, Dict, Tuple, Callable, MutableMapping, Set

from dbt.config import read_user_config, RuntimeConfig
from dbt.contracts.files import SchemaSourceFile, AnySourceFile
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedDocumentation
from dbt.graph import Graph
from networkx import DiGraph
from collections import defaultdict

from dbt_doctools.markdown_ops import DocsBlock

IdSetMap = Dict[str, Set[str]]

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

    block_id_to_model_ids, model_id_to_block_ids, block_id_to_file_ids = build_docs_block_to_ref_map(manifest, config.project_name)
    block_id_to_source_ids, source_id_to_block_ids, block_id_to_file_ids = build_docs_block_to_source_map(manifest, config.project_name)

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
    _, ref_to_docs, _ = build_docs_block_to_ref_map(manifest, config.project_name)
    _, source_to_docs, _ = build_docs_block_to_source_map(manifest, config.project_name)

    def get_blocks_iter(n):
        return itertools.chain(source_to_docs.get(n, []), ref_to_docs.get(n, []))

    doc_depth = compute_doc_depth(graph.graph, get_blocks_iter)

    def sort_key(doc_name):
        return (doc_depth.get(doc_name, 2 ** 31), doc_name)

    return sort_key


def compute_doc_depth(g, get_blocks_iter: Callable[[str], Iterable[str]]):
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


def build_docs_block_to_ref_map(manifest:Manifest, project_name: str) -> Tuple[IdSetMap, IdSetMap, IdSetMap]:
    block_id_to_model_ids = defaultdict(set)
    model_id_to_block_ids = defaultdict(set)
    block_id_to_file_ids = defaultdict(set)

    for fid, f in manifest.files.items():
        if isinstance(f, SchemaSourceFile) and f.node_patches:
            for ref_dfy in itertools.chain(f.dict_from_yaml.get('models', []), f.dict_from_yaml.get('seeds', [])):
                ref_id = manifest.ref_lookup.get_unique_id(ref_dfy['name'], project_name)
                for column_dfy in ref_dfy.get('columns', []):
                    referenced_block_names = DocsBlock.referenced_doc_names(column_dfy.get('description', ''))
                    for b in referenced_block_names:
                        block_id_to_model_ids[b].add(ref_id)
                        model_id_to_block_ids[ref_id].add(b)
                        block_id_to_file_ids[b].add(fid)

    return block_id_to_model_ids, model_id_to_block_ids, block_id_to_file_ids


def build_docs_block_to_source_map(manifest, project_name: str) -> Tuple[IdSetMap, IdSetMap, IdSetMap]:
    block_id_to_source_ids = defaultdict(set)
    source_id_to_block_ids = defaultdict(set)
    block_id_to_file_ids = defaultdict(set)

    for fid, f in manifest.files.items():
        if isinstance(f, SchemaSourceFile) and f.sources:
            for source_dfy in f.dict_from_yaml['sources']:
                for table_dfy in source_dfy.get('tables', []):
                    source_id = manifest.resolve_source(source_dfy['name'], table_dfy['name'], project_name,
                                                        None).unique_id
                    for column_dfy in table_dfy.get('columns', []):
                        referenced_block_names = DocsBlock.referenced_doc_names(column_dfy.get('description', ''))
                        for b in referenced_block_names:
                            block_id_to_source_ids[b].add(source_id)
                            source_id_to_block_ids[source_id].add(b)
                            block_id_to_file_ids[b].add(fid)

    return block_id_to_source_ids, source_id_to_block_ids, block_id_to_file_ids
