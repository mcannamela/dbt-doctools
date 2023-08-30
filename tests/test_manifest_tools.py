import pytest
from dbt.contracts.files import SchemaSourceFile
from dbt.node_types import NodeType

from dbt_doctools.manifest_tools import build_docs_block_to_ref_map, iter_node_or_sources_files, \
    iter_schema_source_files, source_id_and_column_extractor, ref_id_and_column_extractor, iter_source_tables, \
    inferred_id_type


def test_inferred_id_type_for_models_and_seeds(manifest):
    for node_id, node in manifest.nodes.items():
        if node.resource_type in {NodeType.Model, NodeType.Seed}:
            assert inferred_id_type(node_id) == node.resource_type
        else:
            with pytest.raises(NotImplementedError):
                inferred_id_type(node_id)

def test_inferred_id_type_for_sources(manifest):
    for source_id, source in manifest.sources.items():
        assert inferred_id_type(source_id) == NodeType.Source

def test_build_docs_block_to_ref_map(manifest, config):
    project_name = config.project_name
    block_id_to_model_ids, model_id_to_block_ids, block_id_to_file_ids = build_docs_block_to_ref_map(
        ref_id_and_column_extractor(manifest, project_name),
        source_id_and_column_extractor(manifest, project_name),
        iter_node_or_sources_files(manifest)
    )

    assert block_id_to_model_ids == {
        'dummy.static_map_value': {'seed.dummy.some_static_map'},
        'dummy.zero_layer__old_value': {'model.dummy.old_zero_layer'},
        'dummy.one_layer__old_value': {'model.dummy.old_one_layer'},
        'dummy.some_old_source__value': {'source.dummy.dummy_sources.some_old_source'}
    }

    assert model_id_to_block_ids == {
        'seed.dummy.some_static_map': {'dummy.static_map_value'},
        'model.dummy.old_zero_layer': {'dummy.zero_layer__old_value'},
        'model.dummy.old_one_layer': {'dummy.one_layer__old_value'},
        'source.dummy.dummy_sources.some_old_source': {'dummy.some_old_source__value'},
    }

    assert block_id_to_file_ids == {
        'dummy.static_map_value': {'dummy://seeds/some_static_map.yml'},
        'dummy.zero_layer__old_value': {'dummy://models/zero_layer/schema.yml'},
        'dummy.one_layer__old_value': {'dummy://models/zero_layer/schema.yml'},
        'dummy.some_old_source__value': {'dummy://models/sources/some_sources.yml'}
    }


def test_ref_id_and_column_extractor(manifest, config):
    project_name = config.project_name
    extractor = ref_id_and_column_extractor(manifest, project_name)
    ref_name = 'old_zero_layer'
    exp_columns = 'fake_columns'
    ref_id, columns = extractor({'name': ref_name, 'columns': exp_columns})
    assert columns == exp_columns
    assert ref_id == f'model.{project_name}.{ref_name}'


def test_ref_id_and_column_extractor_ok_when_columns_dne(manifest, config):
    project_name = config.project_name
    extractor = ref_id_and_column_extractor(manifest, project_name)
    ref_name = 'old_zero_layer'
    ref_id, columns = extractor({'name': ref_name})
    assert columns == []
    assert ref_id == f'model.{project_name}.{ref_name}'


def test_source_id_and_column_extractor(manifest, config):
    project_name = config.project_name
    extractor = source_id_and_column_extractor(manifest, project_name)

    source_name = 'dummy_sources'
    table_name = 'some_old_source'
    exp_columns = 'fake_columns'
    ref_id, columns = extractor((source_name, {'name': table_name, 'columns': exp_columns}))
    assert columns == exp_columns
    assert ref_id == f'source.{project_name}.{source_name}.{table_name}'


def test_source_id_and_column_extractor_ok_when_columns_dne(manifest, config):
    project_name = config.project_name
    extractor = source_id_and_column_extractor(manifest, project_name)

    source_name = 'dummy_sources'
    table_name = 'some_old_source'
    ref_id, columns = extractor((source_name, {'name': table_name}))
    assert columns == []
    assert ref_id == f'source.{project_name}.{source_name}.{table_name}'


def test_iter_schema_source_files(manifest):
    files_iter = iter_schema_source_files(manifest)
    for fid, f in files_iter:
        assert isinstance(f, SchemaSourceFile)
        assert manifest.files[fid] == f


def test_iter_node_or_sources_files(manifest):
    files_iter = iter_node_or_sources_files(manifest)
    for fid, f in files_iter:
        assert f.node_patches or f.sources
        assert manifest.files[fid] == f


def test_iter_node_or_sources_files_subset_of_iter_schema_source_files(manifest):
    schema_fids = {fid for fid, _ in iter_schema_source_files(manifest)}
    nodes_fids = {fid for fid, _ in iter_node_or_sources_files(manifest)}
    assert nodes_fids <= schema_fids


def test_iter_source_tables(manifest, config):
    source_tables = list(iter_source_tables(manifest.files['dummy://models/sources/some_sources.yml']))
    assert {(source_name, table['name']) for source_name, table in source_tables} == {('dummy_sources', 'some_old_source'), ('dummy_sources', 'some_new_source'), }
    for _, table in source_tables:
        assert 'columns' in table



