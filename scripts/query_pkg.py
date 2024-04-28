import sqlite3

prefixes_db = sqlite3.connect("package_prefixes.db")


def get_package_prefixes(package_name):
    cursor = prefixes_db.cursor()
    cursor.execute(
        """
        SELECT prefix FROM package_prefixes
        WHERE package = ?
    """,
        (package_name,),
    )
    prefixes = [row[0] for row in cursor.fetchall()]
    return prefixes


if __name__ == "__main__":
    while True:
        print(get_package_prefixes(input("Name: ")))
