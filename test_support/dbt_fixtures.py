from pathlib import Path
from pytest import fixture

from dbt_doctools.dbt_api import obtain_manifest


@fixture()
def manifest_and_graph():
    m, g = obtain_manifest(
        dbt_command_args=['--project-dir', str(Path(__file__).parent.parent / 'dummy_dbt_project' / 'dummy')]
    )
    return m, g


@fixture()
def manifest(manifest_and_graph):
    return manifest_and_graph[0]


@fixture()
def graph(manifest_and_graph):
    return manifest_and_graph[1]
