from pytest import mark

from dbt_doctools.markdown_ops import DocsBlock


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
                      ("blah blah{{doc('ref_name_123')}}#blahblah ", ['ref_name_123']),
                      ("{{ doc( 'ref_name' ) }}", ['ref_name']),
                      ('{{ doc("ref_name") }}', ['ref_name']),
                      ('{{ doc("ref_name\') }}', []),
                      ("{{ duc( 'ref_name' ) }}", []),
                      ("{ doc( 'ref_name' ) }}", []),
                      ("{{ doc( 'ref_name' ) }", []),
                      ("blah blah{{doc('ref_1')}}#blahblah{{doc('ref_2')}}", ['ref_1', 'ref_2']),
                      ("{{doc('ref_1')}}{{doc('ref_1')}}", ['ref_1', 'ref_1']),
                  ])
def test_referenced_doc_names(content, exp_doc_names):
    assert DocsBlock.referenced_doc_names(content) == exp_doc_names