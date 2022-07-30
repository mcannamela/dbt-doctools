from dbt.contracts.files import SchemaSourceFile
from dbt.contracts.graph.parsed import ParsedSourceDefinition

from dbt_doctools.source_ops import extract_source


def test_extract_source(manifest, config):
    s, f = extract_source(manifest, config, 'sources', 'some_old_source')
    assert isinstance(s, ParsedSourceDefinition)
    assert isinstance(f, SchemaSourceFile)