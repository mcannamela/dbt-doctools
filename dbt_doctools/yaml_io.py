from dbt.contracts.files import SchemaSourceFile
from loguru import logger
from yaml import dump

from dbt_doctools.yaml_ops import YamlFragment


def overwrite_yaml_file(yaml_doc: YamlFragment, schema_file: SchemaSourceFile):
    path = schema_file.path.full_path
    logger.info(f"Overwriting yaml at {path}")
    with open(path, 'w') as f:
        dump(yaml_doc, f)


def create_or_append_companion_markdown(content: str, schema_file: SchemaSourceFile):
    path = schema_file.path.full_path.replace('.yml', '.md').replace('.yaml', '.md')
    logger.info(f"Writing companion markdown to {path}")
    with open(path, 'wa') as f:
        f.write(content)
