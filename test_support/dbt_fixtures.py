from pathlib import Path
from pytest import fixture

from dbt_doctools.dbt_api import obtain_manifest_and_graph_and_config
from git import Repo
import sh
def repo_top_level():
    return Path(__file__).parent.parent

def dbt_dummy_project_path():
    return repo_top_level() / 'dummy_dbt_project' / 'dummy'

@fixture()
def dummy_project_files():
    sh.git(*(f"config --global --add safe.directory {repo_top_level()}".split()))
    sh.git(*(f"restore --source=HEAD --staged --worktree -- {dbt_dummy_project_path()}".split()))
    yield
    sh.git(*(f"restore --source=HEAD --staged --worktree -- {dbt_dummy_project_path()}".split()))


@fixture()
def manifest_and_graph_and_config(dummy_project_files):
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
