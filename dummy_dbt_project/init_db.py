from test_support.db_fixtures import database_name, db_cursor, attempt_and_recover_if_database_dne, connect_to_test_db, \
    create_database
from loguru import logger

def raise_if_db_dne():
    conn = attempt_and_recover_if_database_dne(connect_to_test_db, create_database)
    with conn.cursor() as cursor:
        cursor.execute("select datname from pg_catalog.pg_database")
        datnames = cursor.fetchall()
    if database_name() not in {t[0] for t in datnames}:
        raise RuntimeError(f"Database {database_name()} not found: {datnames}")
    logger.info(f"Database {database_name()} found OK")

if __name__ == '__main__':
    raise_if_db_dne()
