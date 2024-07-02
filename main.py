import pathlib
from concurrent.futures import ThreadPoolExecutor

from package_database import PackageDatabase
from pypi_scraper import PyPIScraper

def _chunked(iterable, n):
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == n:
            yield chunk
            chunk = []

    if chunk:  # Yield the last chunk if it's not empty
        yield chunk

def process_package(db, scraper, package_name, package_pos):
    print(package_name)
    result = scraper.scrape_package(package_name)
    if result:
        wheel_info, filepaths = result
        db.insert_package(**wheel_info, package_pos=package_pos, filepaths=filepaths)
        print(f"Finished processing {package_name}")
    else:
        print(f"No suitable package found for {package_name}")

def process_duplicates(db, scraper, package_name, url, filepaths):
    print(url, len(filepaths))
    namespaces = set(scraper.is_explicit_namespace_package(url, filepaths))
    for filepath in filepaths:
        db.check_and_store_namespace_package(package_name, filepath, filepath in namespaces)

# @TODO: Lowest common ancestor
def find_nested_common_prefixes(paths):
    parents = set((path.parent if path.stem == "__init__" else path.with_suffix("")) for path in paths)
    for parent in sorted(parents, key=lambda p: len(p.parts)):
        for ancestor in parent.parents:
            if ancestor in parents:
                parents.remove(parent)
                break

    return sorted(parents)

def main():
    db = PackageDatabase()
    db.create_tables()
    scraper = PyPIScraper()

    with ThreadPoolExecutor(max_workers=20) as executor:
        packages = pathlib.Path("packages.txt").read_text().splitlines()
        packages = [scraper.normalize(pkg) for pkg in packages]
        pos_by_pkg = {pkg: i+1 for i, pkg in enumerate(packages)}
        missing_packages = db.get_missing_packages(packages)

        print(f"launching {len(missing_packages)} tasks")
        for chunk in _chunked(missing_packages, 100):
            list(executor.map(lambda pkg: process_package(db, scraper, pkg, pos_by_pkg[pkg]), chunk))

        # =====

        duplicate_filepaths = db.get_duplicate_dunder_inits()
        print(len(duplicate_filepaths))
        filepaths_by_url = db.get_missing_dup_filepaths_by_url(duplicate_filepaths)
        for chunk in _chunked(filepaths_by_url.items(), 10):
            list(executor.map(lambda x: process_duplicates(db, scraper, x[0][0], x[0][1], x[1]), chunk))

        # =====
        prefixes = {}
        for package, filepaths in db.iterate_filepaths():
            filepaths = [
                filepath for filepath in filepaths
                if not any(filepath.startswith(prefix) for prefix in ("test/", "tests/", "doc/", "docs/", "example/", "examples/", "benchmark/", "benchmarks/", "script/", "scripts/", "bin/", "samples/"))
            ]
            prefixes[package] = list(map(str, find_nested_common_prefixes(list(map(pathlib.Path, filepaths)))))

        for package, prefix_list in prefixes.items():
            db.insert_package_prefixes(package, prefix_list)

if __name__ == "__main__":
    main()
