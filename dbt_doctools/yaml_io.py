from typing import Dict

from dbt.contracts.files import SchemaSourceFile
from dbt.contracts.graph.manifest import Manifest
from loguru import logger
from oyaml import dump

from dbt_doctools.markdown_ops import DocsBlock
from dbt_doctools.source_ops import maybe_extract_companion_markdown_file
from dbt_doctools.yaml_ops import YamlFragment, ordered_fragment, apply_to_leaves


def dbt_sort_key(yaml_key: str):
    prefix = str(
        {'version': 0, 'name': 1, 'description': 2, 'tables': 3, 'columns': 4, 'tests': 5, }.get(yaml_key, 999))
    return '_'.join([prefix, yaml_key])


def overwrite_yaml_file(yaml_doc: YamlFragment, schema_file: SchemaSourceFile):
    path = schema_file.path.full_path
    logger.info(f"Overwriting yaml at {path}")
    clean_yaml_doc = apply_to_leaves(yaml_doc, lambda s: s.strip('\n') if isinstance(s, str) else s)
    ordered_yaml_doc = ordered_fragment(clean_yaml_doc, lambda t: dbt_sort_key(t[0]))

    with open(path, 'w') as f:
        dump(ordered_yaml_doc, f)


def create_or_append_companion_markdown(text_blocks: Dict[str, DocsBlock], schema_file: SchemaSourceFile,
                                        manifest: Manifest):
    path = schema_file.path.full_path.replace('.yml', '.md').replace('.yaml', '.md')
    maybe_file = maybe_extract_companion_markdown_file(manifest, schema_file)
    if maybe_file is not None:
        logger.info(f"Companion markdown file {maybe_file.path.full_path} already exists! Appending.")
        mode = 'a'
        preamble = '\n\n'
        existing_block_names = {d.name for d in manifest.docs.values() if d.file_id == maybe_file.file_id}
        logger.info(f"Already have blocks: {existing_block_names}")
        skipped_blocks = {b.name for b in text_blocks.values()} - existing_block_names
        logger.info(f"Existing blocks will not be overwritten: {skipped_blocks}")
        filtered_blocks = [b for b in text_blocks.values() if b.name not in existing_block_names]
    else:
        mode = 'w'
        preamble = ''
        filtered_blocks = text_blocks.values()

    markdown_content = '\n\n'.join([b.rendered for b in filtered_blocks])
    logger.debug(f'extracted docs blocks: \n{markdown_content}')
    logger.info(f"Writing companion markdown to {path}")
    with open(path, mode) as f:
        f.write(preamble)
        f.write(markdown_content)
