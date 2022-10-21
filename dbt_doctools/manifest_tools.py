import itertools
from collections import defaultdict
from typing import Iterable, Dict, Tuple, Callable, Set

from dbt.contracts.files import SchemaSourceFile
from dbt.contracts.graph.manifest import Manifest

from dbt_doctools.markdown_ops import DocsBlock
from dbt_doctools.yaml_ops import YamlMap, YamlList

IdSetMap = Dict[str, Set[str]]


def build_docs_block_to_ref_map(
        extract_ref_id_and_columns: Callable[[YamlMap], Tuple[str, YamlList]],
        extract_source_id_and_columns: Callable[[Tuple[str, YamlMap]], Tuple[str, YamlList]],
        node_or_sources_files_iter: Iterable[SchemaSourceFile]
) -> Tuple[IdSetMap, IdSetMap, IdSetMap]:
    block_id_to_model_ids = defaultdict(set)
    ref_id_to_block_ids = defaultdict(set)
    block_id_to_file_ids = defaultdict(set)

    for fid, f in node_or_sources_files_iter:
        ref_or_source_iter = itertools.chain(
            zip(f.dict_from_yaml.get('models', []), itertools.repeat(extract_ref_id_and_columns)),
            zip(f.dict_from_yaml.get('seeds', []), itertools.repeat(extract_ref_id_and_columns)),
            zip(iter_source_tables(f), itertools.repeat(extract_source_id_and_columns)),
        )
        for dfy, extract_fun in ref_or_source_iter:
            ref_or_source_id, columns_iter = extract_fun(dfy)
            for column_dfy in columns_iter:
                for block_id in DocsBlock.iter_block_ids(DocsBlock.referenced_doc_names(column_dfy.get('description', '')), f.project_name):
                    block_id_to_model_ids[block_id].add(ref_or_source_id)
                    ref_id_to_block_ids[ref_or_source_id].add(block_id)
                    block_id_to_file_ids[block_id].add(fid)

    return block_id_to_model_ids, ref_id_to_block_ids, block_id_to_file_ids


def ref_id_and_column_extractor(manifest:Manifest, project_name:str)->Callable[[YamlMap], Tuple[str, YamlList]]:
    def extract_ref_id_and_columns(y:YamlMap)->Tuple[str, YamlList]:
        return manifest.ref_lookup.get_unique_id(y['name'], project_name), y.get('columns',[])

    return extract_ref_id_and_columns


def source_id_and_column_extractor(manifest:Manifest, project_name:str)->Callable[[Tuple[str, YamlMap]], Tuple[str, YamlList]]:
    def extract_source_id_and_columns(t: Tuple[str, YamlMap]) -> Tuple[str, YamlList]:
        source_name = t[0]
        y = t[1]
        return manifest.resolve_source(source_name, y['name'], project_name, None).unique_id, y.get('columns', [])

    return extract_source_id_and_columns


def iter_schema_source_files(manifest:Manifest)->Iterable[SchemaSourceFile]:
    for fid, f in manifest.files.items():
        if isinstance(f, SchemaSourceFile):
            yield fid, f


def iter_node_or_sources_files(manifest:Manifest)->Iterable[SchemaSourceFile]:
    for fid, f in iter_schema_source_files(manifest):
        if f.node_patches or f.sources:
            yield fid, f


def iter_source_tables(f: SchemaSourceFile)->Iterable[Tuple[str, YamlMap]]:
    return itertools.chain(*(((s['name'], t) for t in s.get('tables', [])) for s in f.dict_from_yaml.get('sources', [])))