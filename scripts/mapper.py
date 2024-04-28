import sqlite3
import re
from pathlib import PurePath

wheel_db = sqlite3.connect("wheel_database.db")
prefixes_db = sqlite3.connect("package_prefixes.db")


def normalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def make_names_table():
    cursor = prefixes_db.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS package_prefixes (
            package TEXT,
            prefix TEXT,
            PRIMARY KEY (package, prefix)
        )
    """)
    prefixes_db.commit()


def get_package_names():
    cursor = wheel_db.cursor()
    cursor.execute("""
        SELECT DISTINCT package_name FROM wheels
    """)
    package_names = set(row[0] for row in cursor.fetchall())
    return package_names


def get_package_count(package):
    cursor = prefixes_db.cursor()
    cursor.execute(
        """
        SELECT COUNT(*) FROM package_prefixes
        WHERE package = ?
    """,
        (package,),
    )
    return cursor.fetchone()[0]


def get_package_filenames(package_name):
    cursor = wheel_db.cursor()
    cursor.execute(
        """
        SELECT filenames.filename
        FROM filenames
        JOIN wheels ON filenames.wheel_id = wheels.id
        WHERE wheels.package_name = ?
    """,
        (package_name,),
    )
    filenames = [row[0] for row in cursor.fetchall()]

    return filenames


def find_nested_common_prefixes(paths):
    parents = set(path.parent for path in paths if len(path.parents) > 1)
    for parent in sorted(parents, key=lambda p: len(p.parts)):
        for ancestor in parent.parents:
            if ancestor in parents:
                parents.remove(parent)
                break

    return sorted(
        set(
            list(parents)
            + [path.with_suffix("") for path in paths if len(path.parents) == 1]
        )
    )


def insert_prefixes(package, prefixes):
    cursor = prefixes_db.cursor()
    for prefix in prefixes:
        cursor.execute(
            """
            INSERT INTO package_prefixes (package, prefix)
            VALUES (?, ?)
        """,
            (package, prefix),
        )
    prefixes_db.commit()


def mapper_main():
    make_names_table()
    for package in get_package_names():
        package = normalize(package)
        count = get_package_count(package)

        if count > 0:
            print(f"Package {package} already exists in the database. Skipping...")
            continue

        print(package)
        filenames = get_package_filenames(package)
        paths = [path for path in map(PurePath, filenames) if path.suffix == ".py"]
        prefixes = list(map(str, find_nested_common_prefixes(paths)))
        insert_prefixes(package, prefixes)


# @TODO: This still doesn't account for explicit namespace packages
#   E.g. foo package -> ["foo/__init__.py", "foo/other.py"]
#       and foo-thing -> ["foo/__init__.py", foo/thing/amabob.py"]
#       where in both `foo/__init__.py` is an explicit namepsace package.
if __name__ == "__main__":
    mapper_main()
