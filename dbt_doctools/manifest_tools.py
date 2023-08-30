import itertools
from collections import defaultdict
from copy import deepcopy
from typing import Iterable, Dict, Tuple, Callable, Set

from dbt.contracts.files import SchemaSourceFile
from dbt.contracts.graph.manifest import Manifest
from dbt.node_types import NodeType

from dbt_doctools.markdown_ops import DocsBlock
from dbt_doctools.yaml_ops import YamlMap, YamlList

IdSetMap = Dict[str, Set[str]]

def inferred_id_type(s: str)->NodeType:
    """Infer the dbt node type from its string identifier

    We are being bad here, but in some places all we have is the string and it would be
    inconvenient to lug the manifest around. It seems like a stable enough pattern for now
    that `seed` identifiers will start with "seed" and so on, so long as we maintain test
    coverage verifying this assumption.
    """
    if s.startswith('source.'):
        return NodeType.Source
    elif s.startswith('model.'):
        return NodeType.Model
    elif s.startswith('seed.'):
        return NodeType.Seed
    else:
        raise NotImplementedError(f"Only source, seed, model are supported, got: {s}")

def build_ref_to_yaml_map(manifest:Manifest):
    """Build maps from ref id to the schema file that documents it and from that file id to the yaml representation"""
    source_id_to_schema_file_id = {}
    ref_id_to_schema_file_id = {}
    file_id_to_yaml_map = {}
    for ref_id, ref in manifest.nodes.items():
        if ref.resource_type in {NodeType.Model, NodeType.Seed}:
            ref_id_to_schema_file_id[ref_id] = ref.patch_path
            file_id_to_yaml_map[ref.patch_path] = deepcopy(manifest.files[ref.patch_path].dict_from_yaml)

    for source_id, source in manifest.sources.items():
        source_id_to_schema_file_id[source_id] = source.file_id
        file_id_to_yaml_map[source.file_id] = deepcopy(manifest.files[source.file_id].dict_from_yaml)

    return source_id_to_schema_file_id, ref_id_to_schema_file_id, file_id_to_yaml_map

def build_docs_block_to_ref_map(
        extract_ref_id_and_columns: Callable[[YamlMap], Tuple[str, YamlList]],
        extract_source_id_and_columns: Callable[[Tuple[str, YamlMap]], Tuple[str, YamlList]],
        node_or_sources_files_iter: Iterable[SchemaSourceFile]
) -> Tuple[IdSetMap, IdSetMap, IdSetMap]:
    """Build maps between docs blocks and the files and models in which they are used

    Args:
        extract_ref_id_and_columns: From a yaml fragment representing a model or seed, obtain the ref's unique id and
            column definitions.
        extract_source_id_and_columns: From a source name and yaml fragment representing one source table, obtain the
            unique id and column definitions for the source table.
        node_or_sources_files_iter: Iterator of `SchemaSourceFiles` from which to build the maps

    Returns:
        block_id_to_model_ids: A map from the id of a `docs` block to a set of model ids that use the `docs` block. The
            ref ids can be used to look up a ref in a `Manifest`'s `nodes`.
        ref_id_to_block_ids: A map from the id of a model or seed to the ids of the `docs` blocks that the ref uses. The
            ref ids can be used to look up a ref in a `Manifest`'s `nodes`.
        block_id_to_file_ids: A map from the id of a `docs` block to a set of file ids in which the blocks appear. The
            file ids can be used to look up the file in a `Manifest`s `files`.

    """
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
    """Construct a function that extracts the id and columns for a model or seed from that ref's yaml fragment

    A yaml fragment for a model or seed (a ref) contains the name of the ref but not the unique id
    that is required to look up information about the ref in other dbt objects, such as the `nodes` of a `Manifest`.

    A `Manifest` can perform the lookup from name to unique id if we also have the name of the dbt project,
    but we don't always wish to pass these around, so this function factory closes the operation of extracting a
    unique id and column definitions from a yaml fragment with respect to the manifest and project name.

    Args:
        manifest: dbt `Manifest` used to look up the unique id
        project_name: Name of the dbt project

    Returns:
        A function from a `YamlMap` representing a model or seed to that model or seed's unique id
        and column definitions.

    """
    def extract_ref_id_and_columns(y:YamlMap)->Tuple[str, YamlList]:
        """Extract unique id and column defintions from a ref's yaml representation

        Args:
            y: A yaml fragment representing a model or seed (a ref)

        Returns:
            A tuple of the unique id for the ref and its column definitions

        """
        return manifest.ref_lookup.get_unique_id(y['name'], project_name), y.get('columns',[])

    return extract_ref_id_and_columns


def source_id_and_column_extractor(manifest:Manifest, project_name:str)->Callable[[Tuple[str, YamlMap]], Tuple[str, YamlList]]:
    """Construct a function that extracts the id and columns for a source table from that source's yaml fragment

        A yaml fragment for a source table contains the name of the source table but not the unique id
        that is required to look up information about the source in other dbt objects, such as the `sources`
        of a `Manifest`.

        A `Manifest` can perform the lookup from name to unique id if we also have the name of the dbt project,
        but we don't always wish to pass these around, so this function factory closes the operation of extracting a
        unique id and column definitions from a source name and yaml fragment with respect to the manifest and
        project name.

        Note the difference between this and `extract_ref_id_and_columns` is that the function we construct requires an
        extra piece of information -- the source name -- in addition to the yaml fragment for the source table for
        which we want the id and column definitions.

        Args:
            manifest: dbt `Manifest` used to look up the unique id
            project_name: Name of the dbt project

        Returns:
            A function from a `YamlMap` representing a model or seed to that model or seed's unique id
            and column definitions.

        """
    def extract_source_id_and_columns(t: Tuple[str, YamlMap]) -> Tuple[str, YamlList]:
        """Extract the unique id and column definitions for a source table from its yaml representation

            Args:
                t: A tuple of the source's name (which could contain many tables) and the yaml fragment
                    for a single table in the source for which we want the unique id and columns

            Returns:
                A tuple of the unique id for the source table and its column definitions

        """
        source_name = t[0]
        y = t[1]
        return manifest.resolve_source(source_name, y['name'], project_name, None).unique_id, y.get('columns', [])

    return extract_source_id_and_columns


def iter_schema_source_files(manifest:Manifest)->Iterable[Tuple[str,SchemaSourceFile]]:
    """Iterate through files in the manifest that are schema i.e. yaml files"""
    for fid, f in manifest.files.items():
        if isinstance(f, SchemaSourceFile):
            yield fid, f


def iter_node_or_sources_files(manifest:Manifest)->Iterable[Tuple[str,SchemaSourceFile]]:
    """Iterate through schema files in the manifest that define a model, seed, snapshot, or analysis"""
    for fid, f in iter_schema_source_files(manifest):
        if f.node_patches or f.sources:
            yield fid, f


def iter_source_tables(f: SchemaSourceFile)->Iterable[Tuple[str, YamlMap]]:
    """Iterate through each table of each dbt source defined in f, returning the parsed yaml form of the table"""
    return itertools.chain(*(((s['name'], t) for t in s.get('tables', [])) for s in f.dict_from_yaml.get('sources', [])))