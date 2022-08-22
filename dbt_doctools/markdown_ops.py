from dataclasses import dataclass
import re

@dataclass(frozen=True)
class DocsBlock:
    name:str
    content:str

    @classmethod
    def source_column_doc_name(cls, source_name, table_name, column_name):
        return '__'.join([source_name, table_name, column_name, 'doc'])

    @classmethod
    def model_column_doc_name(cls, model_name, column_name):
        return '__'.join([model_name, column_name, 'doc'])

    @classmethod
    def doc_ref_regex(cls):
        return re.compile(r'{{\s*doc\s*\(\s*([\'\"])[a-zA-Z0-9_]+\s*\1\)\s*}}')

    @classmethod
    def contains_doc_ref(cls, content:str):
        return cls.doc_ref_regex().match(content) is not None

    def doc_ref(self, comment=None):
        c = '' if comment is None else f' # {comment}'
        return f"{{{{ doc('{self.name}'){c} }}}}"

    @property
    def rendered(self):
        return f"{{% docs {self.name} %}}\n{self.content}\n{{% enddocs %}}"

    @property
    def is_already_docs_block(self)->bool:
        return self.contains_doc_ref(self.content)