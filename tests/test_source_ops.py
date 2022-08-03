from dbt.contracts.files import SchemaSourceFile
from dbt.contracts.graph.parsed import ParsedSourceDefinition

from dbt_doctools.source_ops import extract_source, refactor_to_doc_blocks, refactor_to_one_file_per_table


def test_extract_source(manifest, config):
    s, f = extract_source(manifest, config.project_name, 'dummy_sources', 'some_old_source')
    assert isinstance(s, ParsedSourceDefinition)
    assert isinstance(f, SchemaSourceFile)


def test_refactor_to_doc_blocks(manifest, config):
    s, f = extract_source(manifest, config.project_name, 'dummy_sources', 'some_old_source')
    thing = refactor_to_doc_blocks(s, f)


def test_refactor_to_one_file_per_table(manifest, config):
    s, f = extract_source(manifest, config.project_name, 'dummy_sources', 'some_old_source')
    thing = [source_yaml for source_yaml in refactor_to_one_file_per_table(f, 'dummy_sources')]