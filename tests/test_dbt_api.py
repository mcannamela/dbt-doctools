from typing import Union, MutableMapping

from dbt.contracts.files import SchemaSourceFile, SourceFile
from dbt.contracts.graph.manifest import Manifest
from dbt.graph import Graph


def test_obtain_manifest(manifest, graph):
    m, g = manifest, graph
    assert isinstance(m, Manifest)
    assert isinstance(g, Graph)
    files: MutableMapping[str, Union[SchemaSourceFile, SourceFile]] = m.files
    x = {k: f for k, f in files.items() if isinstance(f, SchemaSourceFile)}
    assert 'dummy://models/sources/some_sources.yml' in x
    y = x['dummy://models/sources/some_sources.yml']
    sources = y.dfy['sources']
    assert len(sources) == 1
    some_old_source = {s['name']: s for s in sources[0]['tables']}['some_old_source']
    column_descriptions = {v['name']: v['description'] for v in some_old_source['columns']}
    assert set(column_descriptions.keys()) == {'old_source_id', 'value'}
