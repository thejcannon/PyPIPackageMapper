import sqlite3

wheel_db = sqlite3.connect("wheel_database.db")


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


if __name__ == "__main__":
    while True:
        print(get_package_filenames(input("Name: ")))
