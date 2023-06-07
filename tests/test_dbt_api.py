from typing import Union, MutableMapping

from dbt.contracts.files import SchemaSourceFile, SourceFile, AnySourceFile
from dbt.contracts.graph.manifest import Manifest
from dbt.graph import Graph
from dbt.config import RuntimeConfig


def test_obtain_manifest_and_graph_and_config(manifest, graph, config):
    m, g, c = manifest, graph, config
    assert isinstance(m, Manifest)
    assert isinstance(g, Graph)
    assert isinstance(c, RuntimeConfig)
    files: MutableMapping[str, AnySourceFile] = m.files
    has_sources = {k: f for k, f in files.items() if isinstance(f, SchemaSourceFile) and f.sources}

    assert 'dummy://models/sources/some_sources.yml' in has_sources
    y = has_sources['dummy://models/sources/some_sources.yml']
    sources = y.dfy['sources']
    assert len(sources) == 1
    some_old_source = {s['name']: s for s in sources[0]['tables']}['some_old_source']
    column_descriptions = {v['name']: v['description'] for v in some_old_source['columns']}
    assert set(column_descriptions.keys()) == {'old_source_id', 'value'}

    assert c.project_name == 'dummy'
