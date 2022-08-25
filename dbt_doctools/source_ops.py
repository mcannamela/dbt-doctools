from typing import Tuple, Dict, Any, TypeVar, Iterable, Callable, Optional, List

from dbt.config import RuntimeConfig
from dbt.contracts.files import SchemaSourceFile
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedSourceDefinition
from loguru import logger

from dbt_doctools.markdown_ops import DocsBlock
from dbt_doctools.yaml_io import overwrite_yaml_file, create_or_append_companion_markdown
from dbt_doctools.yaml_ops import unsafe_get_matching_singleton_by_key, YamlFragment
from copy import deepcopy

from dbt.config import read_user_config, RuntimeConfig

from dbt.graph import Graph


def extract_source(m: Manifest, project_name: str, source_name: str, table_name: str) -> Tuple[
    ParsedSourceDefinition, SchemaSourceFile]:
    """Extract the dbt source definition and schema file in which it resides from a whole Manifest

    A `ParsedSourceDefinition` describes one table in a dbt source. It holds the description for the table and its columns
    plus other fields you can define for a source, and has all templating and context-dependent pieces filled in.

    Additionally, we use the source definition to locate the actual yaml file in the project from which the source
    definition was parsed. Importantly, the `SchemaSourceFile` has the Python representation of the yaml file, before
    any templating has been expanded.

    Args:
        m: A fully-loaded dbt Manifest
        project_name: Project in which the source resides
        source_name: Name of the source within the project
        table_name: Name of the table within the source

    Returns:
        A tuple of the source definition and schema file
    """
    has_sources = {k: f for k, f in m.files.items() if isinstance(f, SchemaSourceFile) and f.sources}
    maybe_s = m.resolve_source(source_name, table_name, project_name, None)
    if maybe_s is not None:
        f = has_sources[maybe_s.file_id]
        return maybe_s, f
    else:
        raise LookupError(
            f"Nothing found for source '{source_name}' table '{table_name}' in project '{project_name}'")


def maybe_extract_companion_markdown_file(manifest: Manifest, file: SchemaSourceFile):
    path = file.path.full_path.replace('.yaml', '.md').replace('.yml', '.md')
    for f in manifest.files.values():
        if path == f.path.full_path:
            return f


def extract_source_yaml_fragment_from_file(file_of_source: SchemaSourceFile, source_name: str) -> YamlFragment:
    """Return the Python representation of the yaml fragment that defines the passed dbt source

    Args:
        file_of_source: dbt yaml file that contains the source
        source_name: name of a dbt source in the file, which may contain many tables

    Returns:
        Python representation of the yaml defining the source named `source_name` in `file_of_source`
    """
    source_dfy = unsafe_get_matching_singleton_by_key(
        ((s['name'], s) for s in file_of_source.dict_from_yaml['sources']),
        source_name
    )[1]
    return source_dfy


def extract_source_table_yaml_fragment_from_file(file_of_source: SchemaSourceFile, source_name: str,
                                                 table_name: str) -> YamlFragment:
    """Return the Python representation of the yaml fragment that defines the passed dbt source table

    Args:
        file_of_source: dbt yaml file that contains the source table
        source_name: name of a dbt source in the file, which may contain many tables
        table_name: name of a table in the dbt source `source_name`

    Returns:
        Python representation of the yaml defining the source table named `table_name` in dbt source
        `source_name` defined in `file_of_source`
    """
    source_dfy = extract_source_yaml_fragment_from_file(file_of_source, source_name)
    source_table_dfy = unsafe_get_matching_singleton_by_key(
        ((s['name'], s) for s in source_dfy['tables']),
        table_name
    )[1]

    return source_table_dfy


def replace_source_table_yaml_fragment(source: YamlFragment, source_table: YamlFragment) -> YamlFragment:
    """Replace the yaml fragment for one source table within a dbt source

    If the `source` does not contain a table with the same name as `source_table`, this is a no-op.

    Args:
        source: Python representation of the dbt source's yaml
        source_table: Python representation of a table within `source` to be replaced

    Returns:
        A new representation of the dbt source with the table corresponding to `source_table` replaced
    """
    new_source = deepcopy(source)
    new_source['tables'] = [source_table if t['name'] == source_table['name'] else t for t in new_source['tables']]
    return new_source


def replace_source_yaml_fragment(sources_file: YamlFragment, source: YamlFragment) -> YamlFragment:
    """Replace the yaml fragment for one source table within a dbt source

    If the `source` does not contain a table with the same name as `source_table`, this is a no-op.

    Args:
        sources_file: Python representation of the dbt source yaml file, which may contain many sources
        source: Python representation of an entire dbt source within `sources_file` to be replaced

    Returns:
        A new representation of the dbt sources file with the source corresponding to `source` replaced
    """
    new_sources_file = deepcopy(sources_file)
    new_sources_file['sources'] = [source if s['name'] == source['name'] else s for s in new_sources_file['sources']]
    return new_sources_file


def extract_column_descriptions_to_doc_blocks(source: ParsedSourceDefinition, file_of_source: SchemaSourceFile) -> \
Tuple[
    YamlFragment, Dict[str, DocsBlock]]:
    """Convert text column descriptions to dbt doc blocks

    Given a dbt source definition whose column descriptions are fully-rendered strings (i.e. without templating),
    replace each description in the source's yaml file with an appropriate reference to a doc block and format
    each description as a matching doc block that can be rendered to a markdown file.

    Column descriptions that already contain doc blocks are left unchanged.

    Args:
        source: dbt source table to be refactored
        file_of_source: file from which the `source` was parsed

    Returns:
        A yaml fragment representing the refactored source definition and a list of `DocsBlocks` that can be rendered
        to markdown.
    """
    source_table_dfy = extract_source_table_yaml_fragment_from_file(file_of_source, source.source_name, source.name)

    text_blocks = {
        c['name']: DocsBlock(DocsBlock.source_column_doc_name(source.source_name, source.name, c['name']),
                             c['description'])
        for c in source_table_dfy['columns']
        if not DocsBlock.contains_doc_ref(c['description'])
    }

    new_source_table_dfy = deepcopy(source_table_dfy)
    for c in new_source_table_dfy['columns']:
        nm = c['name']
        if nm in text_blocks:
            c['description'] = text_blocks[nm].doc_ref()

    return new_source_table_dfy, text_blocks


def refactor_to_one_file_per_table(file_of_source: SchemaSourceFile, source_name: str) -> Iterable[YamlFragment]:
    source_dfy = extract_source_yaml_fragment_from_file(file_of_source, source_name)
    source_header = {k: v for k, v in source_dfy.items() if k != 'tables'}
    for table_yaml in source_dfy['tables']:
        yield dict(
            version=file_of_source.dict_from_yaml['version'],
            sources=[dict(**source_header, **dict(tables=table_yaml))]
        )


def refactor_source_to_docs_blocks(config: RuntimeConfig, manifest: Manifest, source_name: str, table_name: str):
    source, file_of_source = extract_source(manifest, config.project_name, source_name, table_name)
    new_source_table_dfy, text_blocks = extract_column_descriptions_to_doc_blocks(source, file_of_source)
    source_dfy = extract_source_yaml_fragment_from_file(file_of_source, source_name)
    new_source_dfy = replace_source_table_yaml_fragment(source_dfy, new_source_table_dfy)
    logger.debug(f'refactored source: \n{new_source_dfy}')
    new_source_file_dfy = replace_source_yaml_fragment(file_of_source.dict_from_yaml, new_source_dfy)
    return file_of_source, manifest, new_source_file_dfy, text_blocks


def write_refactored_source_and_markdown(file_of_source: SchemaSourceFile, manifest: Manifest,
                                         new_source_file_dfy: YamlFragment, text_blocks: Dict[str, DocsBlock]):
    overwrite_yaml_file(new_source_file_dfy, file_of_source)
    create_or_append_companion_markdown(text_blocks, file_of_source, manifest)
