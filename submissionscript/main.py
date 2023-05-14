#!/usr/bin/env python3
import glob
import logging
import os
import re
import sys

import mysql.connector

logger = logging.getLogger("mini-migrator")
logging.basicConfig(level="INFO")

MIGRATION_TABLE = "versionTable"

GET_VERSION_QUERY = f"SELECT MAX(version) as max from {MIGRATION_TABLE}"

INSERT_MIGRATION = "INSERT INTO versionTable VALUES (%s)"

def get_version_number(filename):
    """
    Super simple regex for grabbing version no.
    """
    match = re.match(r"^(\d+)", filename)
    if match:
        return int(match.group())


def parse_version_numbers(files):
    """
    Get version numbers from files
    Uses regex - proceed with caution.

    If a migration by that nubmer exists - error and quit.
    """
    output = {}
    for file in files:
        _, filename = os.path.split(file)
        version = get_version_number(filename)
        if version and not output.get(version, False):
            output[version] = file
        elif output.get(version):
            raise Exception("Multi migration with the same number found.")
    return output


def find_migrations(folder):
    """
    Assume root dir
    """
    root_path = os.path.join("/", folder)
    migration_files = glob.glob(f"{root_path}/*sql")
    logger.info("Found migrations %s", migration_files)
    return parse_version_numbers(migration_files)


def find_migration_version(connection):
    """
    Get max migration version from table
    """
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(GET_VERSION_QUERY)
        rows = cursor.fetchall()
        if len(rows):
            version = rows[0].get("max")
            logger.info("Migration version %s", version)
            return int(version)
        else:
            raise Exception("No rows found in migration table")
    except Exception as e:
        logger.error("Hit exception getting version - exiting")
        raise e


def insert_migration(version, conn):
    """
    Update migration tracking table.
    """
    cursor = conn.cursor()
    logger.info(INSERT_MIGRATION)
    cursor.execute(INSERT_MIGRATION, (version,))


def run_migration_file(migration_file, update_version, conn):
    """
    For a given file, pop it out into a string, 
    run it and add the version increment.
    Should be within a transaction - unclear if currently functional.
    """
    try:
        conn.autocommit = False
        cursor = conn.cursor()
        with open(migration_file) as sql_file:
            sql_query = sql_file.read()
            logger.info("Attempting %s", migration_file)
            logger.info(sql_query)
            cursor.execute(sql_query)
            insert_migration(update_version, conn)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def run_migrations(migrations, current_version, conn):
    versions = sorted(migrations.keys())
    for version in versions:
        if version > current_version:
            run_migration_file(migrations[version], version, conn)


def main():
    """
    Not using arg parse as per usual - highly discouraged.
    """
    if len(sys.argv) == 6:
        _, folder, user, hostname, database, password = sys.argv
    else:
        raise NotImplementedError(
            "Not implemented for extra args, must be set as per example"
        )
    db_connection = mysql.connector.connect(
        host=hostname, user=user, passwd=password, database=database,
        autocommit=False
    )
    version = find_migration_version(db_connection)
    migrations = find_migrations(folder)
    run_migrations(migrations, version, db_connection)


if __name__ == "__main__":
    main()
