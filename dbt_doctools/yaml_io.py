from dbt.contracts.files import SchemaSourceFile
from dbt.contracts.graph.manifest import Manifest
from loguru import logger
from yaml import dump

from dbt_doctools.source_ops import maybe_extract_companion_markdown_file
from dbt_doctools.yaml_ops import YamlFragment


def overwrite_yaml_file(yaml_doc: YamlFragment, schema_file: SchemaSourceFile):
    path = schema_file.path.full_path
    logger.info(f"Overwriting yaml at {path}")
    with open(path, 'w') as f:
        dump(yaml_doc, f)


def create_or_append_companion_markdown(content: str, schema_file: SchemaSourceFile, manifest: Manifest):
    path = schema_file.path.full_path.replace('.yml', '.md').replace('.yaml', '.md')
    maybe_file = maybe_extract_companion_markdown_file(manifest, schema_file)
    if maybe_file is not None:
        logger.info(f"Companion markdown file {maybe_file.path.full_path} already exists! Appending.")
        mode = 'a'
        preamble = '\n\n'
    else:
        mode = 'w'
        preamble=''
    logger.info(f"Writing companion markdown to {path}")
    with open(path, mode) as f:
        f.write(preamble)
        f.write(content)
