from pathlib import Path
from pytest import fixture

from dbt_doctools.dbt_api import obtain_manifest_and_graph_and_config
from git import Repo

def repo_top_level():
    return Path(__file__).parent.parent

def dbt_dummy_project_path():
    return repo_top_level() / 'dummy_dbt_project' / 'dummy'

def dbt_dummy_template():
    return repo_top_level() / 'dummy_dbt_project' / 'dummy'


@fixture()
def markdown_files():
    markdowns = [p.relative_to(repo_top_level()) for p in dbt_dummy_project_path().rglob('*.md')]
    yield markdowns
    reset_files_to_index(markdowns)

@fixture()
def yaml_files():
    yamls = dbt_dummy_project_path().rglob('*.yml')
    yield yamls
    reset_files_to_index(yamls)


def reset_files_to_index(yamls):
    repo = Repo(repo_top_level())
    repo.index.checkout(yamls, force=True)


@fixture()
def manifest_and_graph_and_config():
    m, g, c = obtain_manifest_and_graph_and_config(
        dbt_command_args=['--project-dir', str(dbt_dummy_project_path())]
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
