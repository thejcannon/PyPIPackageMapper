import pathlib
import sqlite3
import zipfile
import requests
import re
from bs4 import BeautifulSoup

from pip._internal.operations.prepare import PipSession
from pip._internal.network.lazy_wheel import LazyZipOverHTTP

session = PipSession()
conn = sqlite3.connect("wheel_database.db")


def normalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def _fetch_filesnames(wheel_url):
    with LazyZipOverHTTP(wheel_url, session) as zf:
        return zipfile.ZipFile(zf, allowZip64=True).namelist()


def create_database():
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wheels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            package_name TEXT,
            package_version TEXT,
            wheel_name TEXT,
            url TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS filenames (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wheel_id INTEGER,
            filename TEXT,
            FOREIGN KEY (wheel_id) REFERENCES wheels (id)
        )
    """)
    # @TODO: Make one more table for explicit namespace packages
    conn.commit()


def insert_wheel(url):
    cursor = conn.cursor()

    wheel_name = url.split("/")[-1].split("#")[0]
    package_name, package_version, *rest = wheel_name.split("-")
    package_name = normalize(package_name)

    cursor.execute(
        """
        INSERT INTO wheels (package_name, package_version, wheel_name, url)
        VALUES (?, ?, ?, ?)
    """,
        (package_name, package_version, wheel_name, url),
    )

    wheel_id = cursor.lastrowid

    filenames = _fetch_filesnames(url)

    for filename in filenames:
        cursor.execute(
            """
            INSERT INTO filenames (wheel_id, filename)
            VALUES (?, ?)
        """,
            (wheel_id, filename),
        )

    conn.commit()


def package_exists(package_name):
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*) FROM wheels WHERE package_name = ?
    """,
        (package_name,),
    )
    count = cursor.fetchone()[0]
    return count > 0


def get_package_filenames(package_name):
    cursor = conn.cursor()
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


def scrape_main():
    create_database()

    for pkgname in pathlib.Path("packages.txt").read_text().splitlines():
        pkgname = normalize(pkgname)
        if package_exists(pkgname):
            continue

        print(f"Looking up {pkgname}")
        response = requests.get(f"https://pypi.org/simple/{pkgname}")
        soup = BeautifulSoup(response.text, "html.parser")
        whl_links = [
            link.get("href")
            for link in soup.find_all("a", href=True)
            if link.get("href").rsplit("#", 1)[0].endswith(".whl")
        ]
        print(f"  Found {len(whl_links)} wheels")
        if whl_links:
            link = whl_links[-1]
            print(f"  Fetching metadata for {link}")
            insert_wheel(link)
            print(f"Finished with {pkgname}")


if __name__ == "__main__":
    scrape_main()
