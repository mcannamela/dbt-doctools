from pathlib import Path
from pytest import fixture

from dbt_doctools.dbt_api import obtain_manifest_and_graph_and_config


@fixture()
def manifest_and_graph_and_config():
    m, g, c = obtain_manifest_and_graph_and_config(
        dbt_command_args=['--project-dir', str(Path(__file__).parent.parent / 'dummy_dbt_project' / 'dummy')]
    )
    return m, g, c


@fixture()
def manifest(manifest_and_graph_and_config):
    return manifest_and_graph_and_config[0]


@fixture()
def graph(manifest_and_graph_and_config):
    return manifest_and_graph_and_config[1]


@fixture()
def config(manifest_and_graph_and_config):
    return manifest_and_graph_and_config[2]
