from loguru import logger

from test_support.db_fixtures import database_name, attempt_and_recover_if_database_dne, connect_to_test_db, \
    create_database


def raise_if_db_dne():
    """Attempt to connect to the test database and create if it does not exist

    Raises only if we fail to connect after attempting to create the database
    """
    conn = attempt_and_recover_if_database_dne(connect_to_test_db, create_database)
    with conn.cursor() as cursor:
        cursor.execute("select datname from pg_catalog.pg_database")
        datnames = cursor.fetchall()
    if database_name() not in {t[0] for t in datnames}:
        raise RuntimeError(f"Database {database_name()} not found: {datnames}")
    logger.info(f"Database {database_name()} found OK")


def create_source_tables():
    conn = connect_to_test_db()
    with conn.cursor() as cursor:
        cursor.execute("create schema if not exists sources")
        cursor.execute("drop table if exists sources.some_new_source, sources.some_old_source")
        cursor.execute("""
            create table sources.some_new_source as
            select new_source_id, new_source_value
            from (values
                      ('x',10),
                      ('y',11),
                      ('z',12)
                  ) as t (new_source_id, new_source_value);
        """)
        cursor.execute("""
                    create table sources.some_old_source as
                    select old_source_id,old_source_value
                    from (values
                              ('m',37),
                              ('n',38),
                              ('o',39)
                          ) as t (old_source_id, old_source_value);
                """)
    conn.commit()
    logger.info("Created source schema and table OK")

if __name__ == '__main__':
    raise_if_db_dne()
    create_source_tables()
