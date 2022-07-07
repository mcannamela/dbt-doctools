import psycopg2


def test_db_connection():
    conn = psycopg2.connect(
        host="db",
        database="dbt_doctools",
        user="postgres",
        password="postgres")