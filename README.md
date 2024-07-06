# PyPIPackageMapper

This repo contains a handul of scripts which help build a database of:
- packages
- filenames in those packages
- duplicate `__init__.py` files, marked as namespace packages or not
- ideally-unique prefixes for a each package (e.g. `pytest` -> [`pytest`, `_pytest`, `py`]

Running `python main.py` in a venv with the reqs is the way to go.

Blog post on the info: https://joshcannon.me/2024/07/05/package-names.html
