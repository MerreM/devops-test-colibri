#!/usr/bin/env python3
import mysql.connector
import sys
import logging
import os
import glob
import re

logger = logging.getLogger("mini-migrator")
logging.basicConfig(level="INFO")

MIGRATION_TABLE = "versionTable"

GET_VERSION_QUERY = f"SELECT MAX(version) as max from {MIGRATION_TABLE}"

INSERT_MIGRATION = "INSERT INTO versionTable (version) VALUES (%s);"

def main():
    parse_args()
    pass

def get_version_number(filename):
    match = re.match(r"^(\d+)", filename)
    if match:
        return match.group()

def parse_version_numbers(files):
    output = {}
    for file in files:
        _, filename = os.path.split(file)
        version = get_version_number(filename)
        if version:
            output[version] = file
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
            return version
        else:
            raise Exception("No rows found in migration table")
    except Exception as e:
        logger.error("Hit exception getting version - exiting")
        raise e

def insert_migration(version, conn):
    cursor = conn.cursor()
    cursor.execute(INSERT_MIGRATION, version)


def run_migration_file(migration_file, update_version, conn):
    try:
        with open(migration_file) as sql_file:
            sql_query = sql_file.read()
            conn.exectute(sql_query)
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

def parse_args():
    """
    Not using arg parse as per usual
    """
    if len(sys.argv) == 6:
        _, folder, user, hostname, database, password = sys.argv
    else:
        raise NotImplementedError("Not implemented for extra args, must be set as per example")
    db_connection = mysql.connector.connect(
        host=hostname, user=user, passwd=password, database=database
    )
    db_connection.autocommit = False
    version = find_migration_version(db_connection)
    migrations = find_migrations(folder)
    run_migrations(migrations, version, db_connection)


if __name__ == "__main__":
    main()
