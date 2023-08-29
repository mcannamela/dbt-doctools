from pathlib import Path

import typer
from loguru import logger

from dbt_doctools.dbt_api import obtain_manifest_and_graph_and_config
from dbt_doctools.docs_ops import consolidate_duplicate_docs_blocks_, propagate_column_descriptions_
from dbt_doctools.source_ops import refactor_source_to_docs_blocks, write_refactored_source_and_markdown

app = typer.Typer()


@app.command()
def refactor_to_docs_blocks(source_name: str, table_name: str, project_dir: str = None):
    """Replace plain text column descriptions in a dbt source table with `doc` references

    For the given source and table, find all column descriptions that do not already contain a `doc` reference, make a
    new `docs` block named "[source_name]__[table_name]__[column_name]__doc" and re-write the source table's yaml
    file with the column descriptions replaced by references to the new docs blocks.

    The new docs blocks are written to a markdown file with the same name as the source's yaml file, placed in the same
    directory as the yaml file. If the markdown file already contains a docs block of the same name it will not be
    overwritten.

    Example:

    ```yaml
    sources:
      - name: my_source
        tables:
          - name: my_table
            columns:
              - name: my_column
                description: This is a column
    ```
    would become:

    ```yaml
    sources:
      - name: my_source
        tables:
          - name: my_table
            columns:
              - name: my_column
                description: "{{ doc('my_source__my_table__my_column__doc')"
    ```
    and the docs block would be added to an adjacent markdown file:
    ```markdown
    {% docs my_source__my_table__my_column__doc %}
    This is a column
    {% enddocs %}
    ```

    Args:
        source_name: Name of the target dbt source
        table_name: Name of a the table within source `source_name` whose column descriptions should be refactored to
            docs blocks.
        project_dir: Path to the target dbt project

    Returns:

    """
    config, _, manifest = _get_manifest_and_graph_and_config(project_dir)
    file_of_source, manifest, new_source_file_dfy, text_blocks = refactor_source_to_docs_blocks(config, manifest,
                                                                                                source_name, table_name)
    write_refactored_source_and_markdown(file_of_source, manifest, new_source_file_dfy, text_blocks)


@app.command()
def consolidate_duplicate_docs_blocks(project_dir: str = None):
    """Combine docs blocks that have identical non-empty content into a single block

    Empty docs blocks i.e. stubs waiting to be filled in are ignored. Blocks referenced at the roots of the DAG are
    preferred to those downstream.

    Args:
        project_dir: Path to the target dbt project

    Returns:

    """
    config, graph, manifest = _get_manifest_and_graph_and_config(project_dir)
    consolidate_duplicate_docs_blocks_(manifest, graph, config)


@app.command()
def propagate_column_descriptions(project_dir: str = None):
    config, graph, manifest = _get_manifest_and_graph_and_config(project_dir)
    propagate_column_descriptions_(manifest, graph, config)


def _get_manifest_and_graph_and_config(project_dir):
    project_dir = str(Path.cwd()) if project_dir is None else project_dir
    logger.info(f"Using project dir: {project_dir}")
    manifest, graph, config = obtain_manifest_and_graph_and_config(
        dbt_command_args=['--project-dir', project_dir]
    )
    return config, graph, manifest


if __name__ == "__main__":
    app()
