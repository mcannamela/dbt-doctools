from dataclasses import dataclass

from dbt.config import Project
from dbt.parser import DocumentationParser
from pytest import mark

from dbt_doctools.markdown_ops import DocsBlock

@dataclass
class FakeProject:
    project_name:str


def test_docs_block_id(manifest, config):
    project = 'some_project'
    block_name = 'some_block'

    p =  DocumentationParser(FakeProject(project), manifest, config)
    assert DocsBlock.id(project, block_name) == p.generate_unique_id(block_name)


@mark.parametrize(('content', 'contains_doc_ref'),
                  [
                      ('{{doc("ref_name")}}', True),
                      ("{{doc('ref_name')}}", True),
                      ("blah blah{{doc('ref_name_123')}}#blahblah ", True),
                      ("{{ doc ( 'ref_name' ) }}", True),
                      ('{{ doc("ref_name") }}', True),
                      ('{{ doc("ref-name") }}', False),
                      ('{{ doc("ref_name ") }}', False),
                      ('{{ doc(" ref_name") }}', False),
                      ('{{ doc("ref_name\') }}', False),
                      ("{{ duc( 'ref_name' ) }}", False),
                      ("{ doc( 'ref_name' ) }}", False),
                      ("{{ doc( 'ref_name' ) }", False),
                      ("{{doc('')}}", False),
                      ("{{doc()}}", False),
                      ("{{doc( )}}", False),

                  ])
def test_contains_doc_ref(content, contains_doc_ref):
    assert DocsBlock.contains_doc_ref(content) is contains_doc_ref


@mark.parametrize(('content', 'exp_doc_names'),
                  [
                      ("blah blah{{doc('ref_name_123')}}#blahblah ", [(None, 'ref_name_123')]),
                      ("{{ doc( 'ref_name' ) }}", [(None, 'ref_name')]),
                      ('{{ doc("ref_name") }}', [(None, 'ref_name')]),
                      ('{{ doc("project_name.ref_name") }}', [("project_name", 'ref_name')]),
                      ('{{ doc("ref_name\') }}', []),
                      ("{{ duc( 'ref_name' ) }}", []),
                      ("{ doc( 'ref_name' ) }}", []),
                      ("{{ doc( 'ref_name' ) }", []),
                      ("blah blah{{doc('ref_1')}}#blahblah{{doc('ref_2')}}", [(None, 'ref_1'), (None, 'ref_2')]),
                      ("{{doc('ref_1')}}{{doc('ref_1')}}", [(None, 'ref_1'), (None, 'ref_1')]),
                      ("{{doc('project_1.ref_1')}}{{doc('project_2.ref_1')}}", [('project_1', 'ref_1'), ('project_2', 'ref_1')]),
                  ])
def test_referenced_doc_names(content, exp_doc_names):
    assert DocsBlock.referenced_doc_names(content) == set(exp_doc_names)