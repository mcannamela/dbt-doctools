from pathlib import Path

import typer
from loguru import logger

from dbt_doctools.dbt_api import obtain_manifest_and_graph_and_config
from dbt_doctools.source_ops import extract_source, refactor_to_doc_blocks, extract_source_yaml_fragment_from_file, \
    replace_source_table_yaml_fragment, \
    replace_source_yaml_fragment
from dbt_doctools.yaml_io import overwrite_yaml_file, create_or_append_companion_markdown

app = typer.Typer()


@app.command()
def refactor_to_docs_blocks(source_name: str, table_name: str, project_dir: str = None):
    project_dir = str(Path.cwd()) if project_dir is None else project_dir
    logger.info(f"Using project dir: {project_dir}")
    manifest, _, config = obtain_manifest_and_graph_and_config(
        dbt_command_args=['--project-dir', project_dir]
    )

    source, file_of_source = extract_source(manifest, config.project_name, source_name, table_name)
    new_source_table_dfy, text_blocks = refactor_to_doc_blocks(source, file_of_source)

    source_dfy = extract_source_yaml_fragment_from_file(file_of_source, source_name)
    new_source_dfy = replace_source_table_yaml_fragment(source_dfy, new_source_table_dfy)
    logger.debug(f'refactored source: \n{new_source_dfy}')

    new_source_file_dfy = replace_source_yaml_fragment(file_of_source.dict_from_yaml, new_source_dfy)

    overwrite_yaml_file(new_source_file_dfy, file_of_source)
    create_or_append_companion_markdown(text_blocks, file_of_source, manifest)


@app.command()
def propagate_column_descriptions(project_dir: str = None):
    raise NotImplementedError()


if __name__ == "__main__":
    app()
