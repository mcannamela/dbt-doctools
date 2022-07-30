from typing import Tuple

from dbt.config import RuntimeConfig
from dbt.contracts.files import SchemaSourceFile
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedSourceDefinition


def blah():
    x = {k: f for k, f in files.items() if isinstance(f, SchemaSourceFile)}
    assert 'dummy://models/sources/some_old_source.yml' in x
    y = x['dummy://models/sources/some_old_source.yml']
    sources = y.dfy['sources']
    assert len(sources) == 1
    some_old_source = {s['name']: s for s in sources[0]['tables']}['some_old_source']
    column_descriptions = {v['name']: v['description'] for v in some_old_source['columns']}
    assert set(column_descriptions.keys()) == {'old_source_id', 'value'}


def extract_source(m: Manifest, c: RuntimeConfig, source_name: str, table_name: str) -> Tuple[
    ParsedSourceDefinition, SchemaSourceFile]:
    has_sources = {k: f for k, f in m.files.items() if isinstance(f, SchemaSourceFile) and f.sources}
    maybe_s = m.resolve_source(source_name, table_name, c.project_name, None)
    if maybe_s is not None:
        f = has_sources[maybe_s.file_id]
        return maybe_s, f
    else:
        raise LookupError(
            f"Nothing found for source '{source_name}' table '{table_name}' in project '{c.project_name}'")


