from test_support.db_fixtures import db_cursor


def test_db_connection(db_cursor):
    """Just show that we can talk to the database at all."""
    db_cursor.execute("SELECT * FROM information_schema.tables")
    tables = db_cursor.fetchall()

    assert 'pg_catalog' in {t[1] for t in tables}
