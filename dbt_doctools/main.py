from pathlib import Path

import typer
from loguru import logger

from dbt_doctools.dbt_api import obtain_manifest_and_graph_and_config
from dbt_doctools.source_ops import extract_source, extract_source_table_yaml_fragment_from_file, \
    refactor_to_doc_blocks, extract_source_yaml_fragment_from_file, replace_source_table_yaml_fragment
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
    source_dfy = extract_source_yaml_fragment_from_file(file_of_source, source_name)
    new_source_table_dfy, text_blocks = refactor_to_doc_blocks(source, file_of_source)
    new_source_dfy = replace_source_table_yaml_fragment(source_dfy, new_source_table_dfy)
    logger.debug(f'refactored source: \n{new_source_dfy}')

    markdown_content = '\n\n'.join([b.rendered for b in text_blocks.values()])
    logger.debug(f'docs blocks: \n{markdown_content}')

    overwrite_yaml_file(new_source_dfy, file_of_source)
    create_or_append_companion_markdown(markdown_content, file_of_source)






@app.command()
def goodbye(name: str, formal: bool = False):
    if formal:
        print(f"Goodbye Ms. {name}. Have a good day.")
    else:
        print(f"Bye {name}!")


if __name__ == "__main__":
    app()
