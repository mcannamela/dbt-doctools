from test_support.db_fixtures import db_conn


def test_db_connection(db_conn):
    with db_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM information_schema.tables")
        tables = cursor.fetchall()

    assert 'pg_catalog' in tables
