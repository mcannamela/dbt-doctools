from typing import Callable, Any

import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.errors import DuplicateDatabase
from psycopg2 import sql

from pytest import fixture
from tenacity import retry, wait_exponential, retry_if_exception_type, retry_if_exception_message
from os import getenv


@fixture
def db_cursor(db_conn):
    with db_conn.cursor() as cursor:
        yield cursor


@fixture(scope='module')
def db_conn():
    yield attempt_and_recover_if_database_dne(connect_to_test_db, create_database)


def attempt_and_recover_if_database_dne(f: Callable[[], Any], recover: Callable[[], Any]) -> Any:
    try:
        val = f()
    except psycopg2.OperationalError as exc:
        if 'database "dbt_doctools_test" does not exist' in str(exc):
            recover()
            val = f()
        else:
            raise
    return val


@retry(retry=retry_if_exception_type(psycopg2.OperationalError) &
             retry_if_exception_message(match='Connection refused'),
       wait=wait_exponential(multiplier=1, min=4, max=10))
def connect_to_test_db():
    return connect_to_db(database=database_name())


def connect_to_db(**kwargs):
    return psycopg2.connect(
        host="db",
        user="postgres",
        password="postgres",
        **kwargs
    )


def create_database():
    conn = connect_to_db()
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql.SQL("create database {}").format(sql.Identifier(database_name())))
        except DuplicateDatabase:
            pass


def database_name():
    k = 'DBT_DATABASE'
    nm = getenv(k)
    if nm is None:
        raise ValueError(f"Be sure to set env var {k}")
    if len(nm) < 3:
        raise ValueError(f"Pick a longer database name to avoid confusion: {k}={nm}")
    return nm
