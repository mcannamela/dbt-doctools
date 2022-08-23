from dbt.contracts.files import SchemaSourceFile
from dbt.contracts.graph.parsed import ParsedSourceDefinition

from dbt_doctools.source_ops import extract_source, refactor_to_doc_blocks, refactor_to_one_file_per_table, \
    extract_source_table_yaml_fragment_from_file, replace_source_table_yaml_fragment, replace_source_yaml_fragment


def test_extract_source(manifest, config):
    s, f = extract_source(manifest, config.project_name, 'dummy_sources', 'some_old_source')
    assert isinstance(s, ParsedSourceDefinition)
    assert isinstance(f, SchemaSourceFile)


def test_refactor_to_doc_blocks(manifest, config):
    s, f = extract_source(manifest, config.project_name, 'dummy_sources', 'some_old_source')
    source_table_dfy = extract_source_table_yaml_fragment_from_file(f, s.source_name, s.name)
    new_source_table_dfy = refactor_to_doc_blocks(s, f)
    assert source_table_dfy.keys() == new_source_table_dfy.keys()
    assert {k: v for k, v in source_table_dfy.items() if k != 'columns'} == {k: v for k, v in
                                                                             new_source_table_dfy.items() if
                                                                             k != 'columns'}
    assert {c['name'] for c in source_table_dfy['columns']} == {c['name'] for c in source_table_dfy['columns']}


def test_refactor_to_one_file_per_table(manifest, config):
    s, f = extract_source(manifest, config.project_name, 'dummy_sources', 'some_old_source')
    thing = [source_yaml for source_yaml in refactor_to_one_file_per_table(f, 'dummy_sources')]
    print(thing)


def test_replace_source_table_yaml_fragment():
    t = {'a': 'b', 'tables': [{'t1': 'v1', 'name': 'first'}, {'t2': 'v2', 'name': 'second'}]}
    f = {'t2': 'v2_prime', 'name': 'second'}
    new_t = replace_source_table_yaml_fragment(t, f)
    assert new_t['tables'][0] == t['tables'][0]
    assert new_t['tables'][1] == f


def test_replace_source_yaml_fragment():
    t = {'a': 'b', 'sources': [{'t1': 'v1', 'name': 'first'}, {'t2': 'v2', 'name': 'second'}]}
    f = {'t2': 'v2_prime', 'name': 'second'}
    new_t = replace_source_yaml_fragment(t, f)
    assert new_t['sources'][0] == t['sources'][0]
    assert new_t['sources'][1] == f
