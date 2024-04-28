import json
import sqlite3
import re


def normalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def is_valid_import_name(name):
    pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*$"
    return re.match(pattern, name) is not None


# Assuming you have already established a connection to the database
connection = sqlite3.connect("package_prefixes.db")
cursor = connection.cursor()

# Query to get the mapping of prefixes to packages for prefixes that appear for multiple packages
query = """
    SELECT prefix, GROUP_CONCAT(package, ', ') as packages
    FROM package_prefixes
    GROUP BY prefix
"""

if True:
    query += """
    HAVING COUNT(DISTINCT package) > 1
"""


cursor.execute(query)
results = cursor.fetchall()

# Create a dictionary to store the prefix -> package mapping
prefix_to_packages = {}

for row in results:
    prefix = row[0].replace("/", ".")
    if not is_valid_import_name(prefix):
        continue
    packages = row[1].split(", ")
    prefix_to_packages[prefix] = list(map(normalize, packages))

# Print the mapping
json_output = json.dumps(prefix_to_packages, indent=2)

# Print the JSON output
print(json_output)

# Close the database connection
connection.close()
