from pathlib import Path
from typing import Dict

from dbt.contracts.files import BaseSourceFile, SchemaSourceFile
from dbt.contracts.graph.manifest import Manifest
from dbt.graph import Graph

from dbt_doctools.dbt_api import obtain_manifest


def test_obtain_manifest():
    m, g = obtain_manifest(dbt_command_args=['--project-dir', str(Path(__file__).parent.parent / 'dummy_dbt_project' / 'dummy')])
    assert isinstance(m, Manifest)
    assert isinstance(g, Graph)
    files: Dict[str, BaseSourceFile] = m.files
    has_docs = {k: f for k, f in m.files.items() if hasattr(f, 'docs') and f.docs}
    x = {k: f for k, f in files.items() if isinstance(f, SchemaSourceFile)}
    assert 'dummy://models/sources/some_old_source.yml' in x
    y = x['dummy://models/sources/some_old_source.yml']
    sources = y.dfy['sources']
    assert len(sources) == 1
    some_old_source = {s['name']:s for s in sources[0]['tables']}['some_old_source']
    column_descriptions = {v['name']: v['description'] for v in some_old_source['columns']}
    assert set(column_descriptions.keys()) == {'old_source_id', 'value'}
