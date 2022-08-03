from dbt_doctools.source_ops import extract_source
from dbt_doctools.yaml_io import write_source


def test_write_source_loopback(manifest, config):
    """Show that we can write a source to the path of our choice and match the existing yaml
    """
    s, f = extract_source(manifest, config.project_name, 'sources', 'some_old_source')
    write_source(s,f)

