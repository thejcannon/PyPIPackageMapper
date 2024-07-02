from pathlib import PurePath
import re
import sqlite3
from collections import defaultdict

def _valid_modname(s):
    return re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", s)

class PackageDatabase:
    def __init__(self, db_path="package_database.sqlite"):
        self.db_path = db_path

    def create_tables(self):
        with sqlite3.connect(self.db_path) as db:
            # FIXME: Add date of URL upload (how recent)
            db.execute("""
                CREATE TABLE IF NOT EXISTS packages (
                    package_name TEXT PRIMARY KEY,
                    package_version TEXT,
                    package_pos UNSIGNED INT,
                    url TEXT
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS filepaths (
                    package_name TEXT,
                    filepath TEXT,
                    PRIMARY KEY (package_name, filepath),
                    FOREIGN KEY (package_name) REFERENCES packages (package_name)
                )
            """)
            db.execute("CREATE INDEX IF NOT EXISTS idx_filepaths_filepath ON filepaths(filepath)")
            db.execute("""
                CREATE TABLE IF NOT EXISTS namespace_packages (
                    package_name TEXT,
                    filepath TEXT,
                    is_namespace BOOLEAN,
                    PRIMARY KEY (package_name, filepath),
                    FOREIGN KEY (package_name) REFERENCES packages (package_name)
                )
            """)
            db.execute("CREATE INDEX IF NOT EXISTS idx_namespace_packages_filepath ON namespace_packages(filepath)")
            db.execute("""
                CREATE TABLE IF NOT EXISTS package_prefixes (
                    package_name TEXT,
                    prefix TEXT,
                    PRIMARY KEY (package_name, prefix),
                    FOREIGN KEY (package_name) REFERENCES packages (package_name)
                )
            """)
            db.execute("CREATE INDEX IF NOT EXISTS idx_package_prefixes_prefix ON package_prefixes(prefix)")

    def get_missing_packages(self, package_names):
        with sqlite3.connect(self.db_path) as db:
            placeholders = ','.join('?' for _ in package_names)
            query = f"SELECT package_name FROM packages WHERE package_name IN ({placeholders})"
            cursor = db.execute(query, package_names)
            existing_packages = set(row[0] for row in cursor.fetchall())
        return [pkg for pkg in package_names if pkg not in existing_packages]

    def insert_package(self, package_name, package_version, url, package_pos, filepaths):
        filepaths = [
            filepath for filepath in filepaths
            if any(
                filepath.endswith(suffix) for suffix in (
                    ".py", ".so", ".dylib", ".pyd"
                )
            ) and ".data/" not in filepath
        ]
        with sqlite3.connect(self.db_path) as db:
            try:
                db.execute("""
                    INSERT OR REPLACE INTO packages (package_name, package_version, package_pos, url)
                    VALUES (?, ?, ?, ?)
                """, (package_name, package_version, package_pos, url))
                db.executemany(
                    "INSERT OR REPLACE INTO filepaths (package_name, filepath) VALUES (?, ?)",
                    [(package_name, filepath) for filepath in filepaths]
                )
            except sqlite3.Error as e:
                e.add_note(f"{package_name=}, {package_version=}, {url=}, {filepaths=}")
                raise


    def get_duplicate_dunder_inits(self):
        with sqlite3.connect(self.db_path) as db:
            query = """
                SELECT filepath
                FROM filepaths
                GROUP BY filepath
                HAVING COUNT(filepath) > 1
                AND filepath LIKE "%/__init__.py"
            """
            cursor = db.execute(query)
            return [row[0] for row in cursor.fetchall()]

    def get_missing_dup_filepaths_by_url(self, dup_filepaths):
        with sqlite3.connect(self.db_path) as db:
            placeholders = ','.join('?' for _ in dup_filepaths)
            query = f"""
                SELECT p.package_name, p.url, f.filepath
                FROM filepaths f
                JOIN packages p ON f.package_name = p.package_name
                WHERE f.filepath IN ({placeholders})
                AND f.filepath NOT IN (
                    SELECT filepath FROM namespace_packages
                )
            """
            result = defaultdict(list)
            cursor = db.execute(query, dup_filepaths)
            rows = cursor.fetchall()
            for row in rows:
                result[(row[0], row[1])].append(row[2])
            return result

    def check_and_store_namespace_package(self, package_name, filepath, is_namespace):
        with sqlite3.connect(self.db_path) as db:
            db.execute("""
                INSERT INTO namespace_packages (package_name, filepath, is_namespace)
                VALUES (?, ?, ?)
            """, (package_name, filepath, is_namespace))

    def iterate_filepaths(self, batch_size=1000):
        with sqlite3.connect(self.db_path) as db:
            query = """
                SELECT
                    package_name,
                    GROUP_CONCAT(filepath, '|') AS filepaths
                FROM (
                    SELECT f.package_name, f.filepath
                    FROM filepaths f
                    LEFT JOIN namespace_packages np ON f.package_name = np.package_name AND f.filepath = np.filepath
                    WHERE np.package_name IS NULL OR np.is_namespace = FALSE
                )
                GROUP BY package_name;
            """
            cursor = db.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    package_name, filepaths_str = row
                    filepaths = filepaths_str.split('|')
                    yield package_name, filepaths

    def insert_package_prefixes(self, package_name, prefixes):
        with sqlite3.connect(self.db_path) as db:
            try:
                db.executemany("""
                    INSERT OR REPLACE INTO package_prefixes (package_name, prefix)
                    VALUES (?, ?)
                """, [(package_name, prefix) for prefix in prefixes])
            except sqlite3.Error as e:
                e.add_note(f"{package_name=}, {prefixes=}")
                raise
