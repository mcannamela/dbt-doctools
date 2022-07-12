import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.errors import DuplicateDatabase
from pytest import fixture
from tenacity import retry, wait_exponential, retry_if_exception_type, retry_if_exception_message

@retry(retry=retry_if_exception_type(psycopg2.OperationalError) &
             retry_if_exception_message(match='Connection refused'),
       wait=wait_exponential(multiplier=1, min=4, max=10))
def connect_to_test_db():
    return psycopg2.connect(
        host="db",
        database='dbt_doctools_test',
        user="postgres",
        password="postgres")

@fixture(scope='module')
def db_conn():
    try:
        conn_with_db = connect_to_test_db()
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

            conn_with_db = connect_to_test_db()
        else:
            raise

    yield conn_with_db


@fixture
def db_cursor(db_conn):
    with db_conn.cursor() as cursor:
        yield cursor