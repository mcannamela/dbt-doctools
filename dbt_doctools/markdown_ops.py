from dataclasses import dataclass, field
import re
from typing import Optional, List
import oyaml as yaml

@dataclass(frozen=True)
class DocsBlock:
    name:str
    content:str

    DOC_REF_REGEX = re.compile(r'{{\s*doc\s*\(\s*([\'\"])(?P<doc_name>[a-zA-Z0-9_]+)\1\s*\)\s*}}')

    @classmethod
    def source_column_doc_name(cls, source_name, table_name, column_name):
        return '__'.join([source_name, table_name, column_name, 'doc'])

    @classmethod
    def model_column_doc_name(cls, model_name, column_name):
        return '__'.join([model_name, column_name, 'doc'])

    @classmethod
    def contains_doc_ref(cls, content:str):
        return cls.DOC_REF_REGEX.search(content) is not None

    @classmethod
    def referenced_doc_names(cls, content:str)->List[str]:
        matches = cls.DOC_REF_REGEX.findall(content)
        return [t[1] for t in matches]

    def doc_ref(self, comment=None):
        return DocRef(self.name, comment)

    @property
    def rendered(self):
        return f"{{% docs {self.name} %}}\n{self.content}\n{{% enddocs %}}"

    @property
    def is_already_docs_block(self)->bool:
        return self.contains_doc_ref(self.content)


@dataclass()
class DocRef:
    name: str
    comment: Optional[str] = field(default='')

    def __str__(self):
        c = '' if self.comment is None else f' # {self.comment}'
        return f"{{{{ doc('{self.name}'){c} }}}}"

    @classmethod
    def represent_as_yaml(cls,dumper, instance):
        return dumper.represent_scalar('tag:yaml.org,2002:str', str(instance), style='"')


def represent_rendered_docref(dumper: yaml.Dumper, instance: str):
    if DocsBlock.contains_doc_ref(instance):
        return dumper.represent_scalar('tag:yaml.org,2002:str', instance, style='"')
    else:
        return dumper.represent_scalar('tag:yaml.org,2002:str', instance)
