from dataclasses import dataclass, field
import re
from typing import Optional, List, Set, Tuple
import oyaml as yaml


@dataclass(frozen=True)
class DocsBlock:
    name: str
    content: str

    DOC_REF_REGEX = re.compile(
        r'{{\s*doc\s*\(\s*([\'\"])((?P<project_name>[a-zA-Z0-9_]+)\.)?(?P<doc_name>[a-zA-Z0-9_]+)\1\s*\)\s*}}'
    )

    @classmethod
    def source_column_doc_name(cls, source_name, table_name, column_name):
        return '__'.join([source_name, table_name, column_name, 'doc'])

    @classmethod
    def model_column_doc_name(cls, model_name, column_name):
        return '__'.join([model_name, column_name, 'doc'])

    @classmethod
    def contains_doc_ref(cls, content: str):
        return cls.DOC_REF_REGEX.search(content) is not None

    @classmethod
    def referenced_doc_names(cls, content: str) -> Set[Tuple[Optional[str], str]]:
        matches = cls.DOC_REF_REGEX.finditer(content)
        return {(m.groupdict()['project_name'], m.groupdict()['doc_name']) for m in matches}

    @classmethod
    def iter_block_ids(cls, maybe_project_name_and_block_names:Set[Tuple[Optional[str], str]], project_name:str):
        for maybe_project_name, block_name in maybe_project_name_and_block_names:
            yield cls.id(
                maybe_project_name if maybe_project_name is not None else project_name,
                block_name
            )

    @classmethod
    def id(cls, project_name: str, block_name: str):
        return f'{project_name}.{block_name}'

    def doc_ref(self, comment=None):
        return DocRef(self.name, comment)

    @property
    def rendered(self):
        return f"{{% docs {self.name} %}}\n{self.content}\n{{% enddocs %}}"

    @property
    def is_already_docs_block(self) -> bool:
        return self.contains_doc_ref(self.content)


@dataclass()
class DocRef:
    name: str
    comment: Optional[str] = field(default='')

    def __str__(self):
        c = '' if self.comment is None else f' # {self.comment}'
        return f"{{{{ doc('{self.name}'){c} }}}}"

    @classmethod
    def represent_as_yaml(cls, dumper, instance):
        return dumper.represent_scalar('tag:yaml.org,2002:str', str(instance), style='"')


def represent_rendered_docref(dumper: yaml.Dumper, instance: str):
    if DocsBlock.contains_doc_ref(instance):
        return dumper.represent_scalar('tag:yaml.org,2002:str', instance, style='"')
    else:
        return dumper.represent_scalar('tag:yaml.org,2002:str', instance)
