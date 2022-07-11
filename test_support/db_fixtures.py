import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.errors import DuplicateDatabase
from pytest import fixture

@fixture(scope='module')
def db_conn():
    try:
        yield psycopg2.connect(
            host="db",
            database='dbt_doctools_test',
            user="postgres",
            password="postgres")
    except psycopg2.OperationalError as exc:
        if 'database "dbt_doctools_test" does not exist' in str(exc):
            conn = psycopg2.connect(
                host="db",
                user="postgres",
                password="postgres")
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as cursor:
                try:
                    cursor.execute("create database dbt_doctools_test")
                except DuplicateDatabase:
                    pass

            conn_with_db = psycopg2.connect(
                host="db",
                database='dbt_doctools_test',
                user="postgres",
                password="postgres")

            yield conn_with_db

@fixture
def db_cursor(db_conn):
    with db_conn.cursor() as cursor:
        yield cursor