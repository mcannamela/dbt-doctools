from typing import Tuple, Dict, Any, TypeVar, Iterable, Callable, Optional

from dbt.config import RuntimeConfig
from dbt.contracts.files import SchemaSourceFile
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedSourceDefinition

from dbt_doctools.yaml_ops import unsafe_get_matching_singleton_by_key, YamlFragment


def blah():
    x = {k: f for k, f in files.items() if isinstance(f, SchemaSourceFile)}
    assert 'dummy://models/sources/some_old_source.yml' in x
    y = x['dummy://models/sources/some_old_source.yml']
    sources = y.dfy['sources']
    assert len(sources) == 1
    some_old_source = {s['name']: s for s in sources[0]['tables']}['some_old_source']
    column_descriptions = {v['name']: v['description'] for v in some_old_source['columns']}
    assert set(column_descriptions.keys()) == {'old_source_id', 'value'}


def extract_source(m: Manifest, project_name, source_name: str, table_name: str) -> Tuple[
    ParsedSourceDefinition, SchemaSourceFile]:
    has_sources = {k: f for k, f in m.files.items() if isinstance(f, SchemaSourceFile) and f.sources}
    maybe_s = m.resolve_source(source_name, table_name, project_name, None)
    if maybe_s is not None:
        f = has_sources[maybe_s.file_id]
        return maybe_s, f
    else:
        raise LookupError(
            f"Nothing found for source '{source_name}' table '{table_name}' in project '{project_name}'")


def extract_source_yaml_fragment_from_file(file_of_source: SchemaSourceFile, source_name: str) -> Any:
    source_dfy = unsafe_get_matching_singleton_by_key(
        ((s['name'], s) for s in file_of_source.dict_from_yaml['sources']),
        source_name
    )[1]
    return source_dfy


def refactor_to_doc_blocks(source: ParsedSourceDefinition, file_of_source: SchemaSourceFile):
    source_dfy = extract_source_yaml_fragment_from_file(file_of_source, source.source_name)

    return source, file_of_source


def refactor_to_one_file_per_table(file_of_source: SchemaSourceFile, source_name: str) -> Iterable[YamlFragment]:
    source_dfy = extract_source_yaml_fragment_from_file(file_of_source, source_name)
    source_header = {k: v for k, v in source_dfy.items() if k != 'tables'}
    for table_yaml in source_dfy['tables']:
        yield dict(
            version=file_of_source.dict_from_yaml['version'],
            sources=[dict(**source_header, **dict(tables=table_yaml))]
        )
