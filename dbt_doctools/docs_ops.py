import itertools
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, List, Dict, Tuple, Callable, MutableMapping

from dbt.config import RuntimeConfig
from dbt.contracts.files import AnySourceFile
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedDocumentation
from dbt.graph import Graph
from dbt.node_types import NodeType
from networkx import DiGraph
from loguru import logger

from dbt_doctools.graph_ops import propagate_breadth_first
from dbt_doctools.manifest_tools import build_docs_block_to_ref_map, ref_id_and_column_extractor, \
    source_id_and_column_extractor, iter_node_or_sources_files, build_ref_to_yaml_map, unsafe_inferred_id_type, \
    inferred_id_type
from dbt_doctools.markdown_ops import DocsBlock, DocRef
from dbt_doctools.source_ops import extract_source_table_yaml_fragment_from_file
from dbt_doctools.yaml_ops import YamlMap, unsafe_get_matching_singleton_by_key


@dataclass
class ColumnPropagationState:
    manifest: Manifest
    source_id_to_schema_file_id: Dict[str, str]
    ref_id_to_schema_file_id: Dict[str, str]
    file_id_to_yaml_map: Dict[str, YamlMap]

    def iter_columns(self, source_or_ref_id:str):
        if unsafe_inferred_id_type(source_or_ref_id) is NodeType.Source:
            s = self.manifest.sources[source_or_ref_id]
            source_name, table_name = s.schema, s.name
            file_yaml = self.file_id_to_yaml_map[self.source_id_to_schema_file_id[source_or_ref_id]]
            yaml = extract_source_table_yaml_fragment_from_file(file_yaml, s.schema, s.name)
            columns = yaml['columns']
        else:
            nm = self.manifest.nodes[source_or_ref_id].name
            yaml = self.file_id_to_yaml_map[self.ref_id_to_schema_file_id[source_or_ref_id]]
            try:
                columns = unsafe_get_matching_singleton_by_key(((m['name'], m) for m in yaml['models']), nm)[1]['columns']
            except:
                raise
        for c in columns:
            yield c['name'], c

    def patch_column_description_if_exists(self, ref_id:str, column_name:str, column_definition:YamlMap):
        if inferred_id_type(ref_id) is not NodeType.Model:
            raise NotImplementedError(f"Patching nodes of this type not supported: {ref_id}")
        nm = self.manifest.nodes[ref_id].name
        yaml = self.file_id_to_yaml_map[self.ref_id_to_schema_file_id[ref_id]]
        columns = unsafe_get_matching_singleton_by_key(((m['name'], m) for m in yaml['models']), nm)[1]['columns']
        if column_name in columns:
            columns[column_name]['description'] = column_definition['description']
            logger.info(f"  Patched column {column_name} in {ref_id}")
        return self

def propagate_column_descriptions_(manifest: Manifest, graph: Graph, config: RuntimeConfig):
    sources = {n for n in graph.graph if graph.graph.in_degree(n) == 0 and inferred_id_type(n) in {NodeType.Source, NodeType.Model, NodeType.Seed}}
    source_id_to_schema_file_id, ref_id_to_schema_file_id, file_id_to_yaml_map = build_ref_to_yaml_map(manifest)
    state = ColumnPropagationState(
        manifest=manifest,
        source_id_to_schema_file_id=source_id_to_schema_file_id,
        ref_id_to_schema_file_id=ref_id_to_schema_file_id,
        file_id_to_yaml_map=file_id_to_yaml_map
    )

    new_state = propagate_breadth_first(graph.graph, sources, state, propagate_doc_block)
    return new_state

def consolidate_duplicate_docs_blocks_(manifest: Manifest, graph: Graph, config: RuntimeConfig):
    """Merge `docs` blocks with identical text

    Find all sets of `docs` blocks that are non-empty (have non-whitespace characters) and
    have identical text, and consolidate them into a single block. The most upstream block
    is preferred.

    Args:
        manifest: dbt `Manifest` for the project
        graph: dbt `Graph` for the project
        config: dbt `RuntimeConfig` that configured the `graph` and `manifest`

    Returns:

    """
    consolidated_docs_and_duplicates = find_consolidated_docs_and_duplicates(manifest, graph, config)

    duplicate_docs_to_remove: List[ParsedDocumentation] = sum([t[1] for t in consolidated_docs_and_duplicates], [])

    doc_file_to_retained_docs, doc_files_to_rewrite = _construct_docs_to_rewrite(duplicate_docs_to_remove, manifest)

    rewrite_doc_files(doc_file_to_retained_docs, doc_files_to_rewrite)

    block_id_to_model_ids, ref_id_to_block_ids, block_id_to_file_ids = build_docs_block_to_ref_map(
        ref_id_and_column_extractor(manifest, config.project_name),
        source_id_and_column_extractor(manifest, config.project_name),
        iter_node_or_sources_files(manifest)
    )

    affected_schema_files = {manifest.files[fid].path.full_path
                             for doc in duplicate_docs_to_remove
                             for fid in block_id_to_file_ids[doc.unique_id]
                             }

    replacements = {
        str(DocRef.from_parsed_doc(consolidated)): [DocRef.from_parsed_doc(d).re() for d in duplicates]
        for consolidated, duplicates in consolidated_docs_and_duplicates
    }
    for file in affected_schema_files:
        logger.info(f"process doc ref replacements in schema file '{file}'")
        with open(file, 'r') as f:
            contents = f.read()
        for replacement, patterns_to_replace in replacements.items():
            for pattern_to_replace in patterns_to_replace:
                contents, n_replaced = re.subn(pattern_to_replace, replacement, contents)
                if n_replaced>0:
                    logger.info(f"  replaced {n_replaced} occurences of '{patterns_to_replace}' with '{replacement}'")
        with open(file, 'w') as f:
            f.write(contents)


def is_non_empty(doc: ParsedDocumentation):
    """True if a doc block contains non-whitespace characters"""
    return len(doc.block_contents.strip()) > 0


def rewrite_doc_files(doc_file_to_retained_docs, doc_files_to_rewrite):
    for doc_file_id, doc_file in doc_files_to_rewrite.items():
        retained_docs = doc_file_to_retained_docs[doc_file_id]
        with open(doc_file.path.absolute_path, 'w') as f:
            for parsed_doc in sorted(retained_docs, key=lambda d: d.unique_id):
                block = DocsBlock.from_parsed_doc(parsed_doc)
                f.write(block.rendered)
                f.write('\n')


def find_consolidated_docs_and_duplicates(manifest: Manifest, graph: Graph, config: RuntimeConfig):
    non_empty_docs = {k: d for k, d in manifest.docs.items() if is_non_empty(d)}
    degenerate_docs = find_degenerate_docs(non_empty_docs)
    sort_key = make_doc_sort_fun(manifest, graph, config)

    def split_degenerates(x: List[ParsedDocumentation]) -> Tuple[ParsedDocumentation, List[ParsedDocumentation]]:
        y = list(sorted(x, key=lambda d: sort_key(d.unique_id)))
        return y[0], y[1:]

    consolidated_docs_and_duplicates = [split_degenerates(x) for x in degenerate_docs]
    return consolidated_docs_and_duplicates


def find_degenerate_docs(non_empty_docs):
    block_content_to_docs: MutableMapping[str, List[ParsedDocumentation]] = defaultdict(list)
    for d in non_empty_docs.values():
        block_content_to_docs[d.block_contents.strip()].append(d)
    degenerate_docs: List[List[ParsedDocumentation]] = [v for v in block_content_to_docs.values() if len(v) > 1]
    return degenerate_docs


def _construct_docs_to_rewrite(duplicate_docs_to_remove: List[ParsedDocumentation], manifest: Manifest) -> Tuple[
    MutableMapping[str, List[ParsedDocumentation]], Dict[str, AnySourceFile]]:
    """Find all files affected by removal of duplicates and list the blocks that should remain after deduplication

    Args:
        duplicate_docs_to_remove: dbt doc blocks that are to be superceded by a block with identical contents
        manifest: dbt Manifest for the project

    Returns:
        A tuple of:
            1. a map from doc (i.e. markdown) file id to the doc blocks that should be left in the file after removing duplicates
            2. a map from doc (i.e. markdown) file id to its dbt `SourceFile`, only for those markdown files that need to be rewritten

    """
    doc_files_to_rewrite: Dict[str, AnySourceFile] = {d.file_id: manifest.files[d.file_id] for d in
                                                      duplicate_docs_to_remove}
    duplicate_doc_ids = {d.unique_id for d in duplicate_docs_to_remove}
    doc_file_to_retained_docs: MutableMapping[str, List[ParsedDocumentation]] = defaultdict(list)
    parsed_docs = manifest.docs.values()
    for d in parsed_docs:
        if d.file_id in doc_files_to_rewrite and d.unique_id not in duplicate_doc_ids:
            doc_file_to_retained_docs[d.file_id].append(d)
    return doc_file_to_retained_docs, doc_files_to_rewrite


def make_doc_sort_fun(manifest: Manifest, graph: Graph, config: RuntimeConfig) -> Callable[[str], Tuple[int, str]]:
    """Build a function that provides a sort key for doc block ids to sort them in order of appearance in the dbt dag
    """
    _, ref_id_to_block_ids, _ = build_docs_block_to_ref_map(
        ref_id_and_column_extractor(manifest, config.project_name),
        source_id_and_column_extractor(manifest, config.project_name),
        iter_node_or_sources_files(manifest)
    )

    def get_blocks_iter(n):
        return ref_id_to_block_ids.get(n, [])

    g: DiGraph = graph.graph
    doc_depth = compute_min_doc_depth(g, get_blocks_iter)

    def sort_key(doc_name: str) -> Tuple[int, str]:
        return doc_depth.get(doc_name, 2 ** 31), doc_name

    return sort_key


def compute_min_doc_depth(g: DiGraph, get_blocks_iter: Callable[[str], Iterable[str]]) -> Dict[str, int]:
    """Find the depth in the dbt graph from the source layer where doc blocks are first used

    Args:
        g: A graph whose nodes are identifiers of dbt models, seeds, or sources
        get_blocks_iter: a function taking a node id and returning the ids of doc blocks that appear in that node's
            documenting yaml

    Returns:
        A dictionary from doc block id to the depth from the source layer where the block first appears
    """
    doc_depth = {}

    node_set = {n for n in g if g.in_degree(n) == 0}
    depth: int = 0
    while node_set:
        for n in node_set:
            for b in get_blocks_iter(n):
                if b not in doc_depth:
                    doc_depth[b] = depth
        node_set = set(itertools.chain(*[g.successors(n) for n in node_set]))
        depth += 1
    return doc_depth

def propagate_doc_block(source_node:str, target_node:str, state:ColumnPropagationState)->ColumnPropagationState:
    source_type_accepted = inferred_id_type(source_node) in {NodeType.Source, NodeType.Seed, NodeType.Model}
    target_type_accepted = inferred_id_type(source_node) in {NodeType.Model}
    if source_type_accepted and target_type_accepted:
        logger.info(f"Patching from {source_node}")
        for source_column_name, source_column_definition in state.iter_columns(source_node):
            state.patch_column_description_if_exists(target_node, source_column_name, source_column_definition)
    else:
        logger.info(f"Not propagating from {source_node} to {target_node} due to node types.")

    return state




