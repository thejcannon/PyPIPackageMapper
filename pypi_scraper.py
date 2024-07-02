from functools import lru_cache
import os
import tarfile
import httpx
from bs4 import BeautifulSoup
import zipfile
import re
import io

from lazy_zip import LazyZipOverHTTP

WS = "\\s*"
EXPLICIT_NS_PKG = re.compile(
    "|".join(
        [
            f"(^.+extend_path\\(__path__,{WS}__name__\\))",
            f"(^.+declare_namespace\\(__name__\\))",
        ]
    ),
    flags=re.MULTILINE
)

@lru_cache(maxsize=1)
def _get_gh_release_map():
    result = {}
    client = httpx.Client()
    url = "https://api.github.com/repos/thejcannon/keeping-it-wheel/releases?per_page=100&page={page}"
    page = 1
    while True:
        response = client.get(url.format(page=page), headers={"Authorization": f"Token {os.getenv('GH_TOKEN', '')}"})
        response.raise_for_status()
        releases = response.json()
        for release in releases:
            name = release["tag_name"].rsplit("-", 1)[0]
            if release["assets"]:
                result[name] = release["assets"][0]["browser_download_url"]
        if "next" in response.headers.get('Link'):
            page +=1
        else:
            return result

class PyPIScraper:
    def __init__(self):
        self.client = httpx.Client()

    @staticmethod
    def normalize(name):
        return re.sub(r"[-_.]+", "-", name).lower()

    def get_wheel_urls(self, package_name):
        normalized_name = self.normalize(package_name)
        response = self.client.get(f"https://pypi.org/simple/{normalized_name}/")
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        hrefs = [
            link.get("href").rsplit("#", 1)[0]
            for link in soup.find_all("a", href=True)
        ]
        return [
            link for link in hrefs if link.endswith(".whl")
        ]


    def scrape_wheel(self, wheel_url):
        wheel_name = wheel_url.split("/")[-1].split("#")[0]
        package_name, package_version, *rest = wheel_name.split("-")
        wheel_info = {
            "package_name": self.normalize(package_name),
            "package_version": package_version,
            "url": wheel_url
        }
        with LazyZipOverHTTP(wheel_url) as zf:
            with zipfile.ZipFile(zf, allowZip64=True) as zip_file:
                filepaths = zip_file.namelist()

        return wheel_info, filepaths

    def scrape_package(self, package_name):
        wheel_urls = self.get_wheel_urls(package_name)
        if not wheel_urls:
            release_map = _get_gh_release_map()
            if url := release_map.get(package_name):
                wheel_urls = [url]
            else:
                return None

        return self.scrape_wheel(wheel_urls[-1])

    def is_explicit_namespace_package(self, url, filepaths):
        result = []

        def handle_file(filepath, content):
            content = content.replace("\r", "")
            if re.search(EXPLICIT_NS_PKG, content):
                result.append(filepath)

        if url.endswith(".tar.gz"):
            response = self.client.get(url)
            response.raise_for_status()
            content = response.content

            with tarfile.open(fileobj=io.BytesIO(content), mode="r:gz") as tar:
                for filepath in filepaths:
                    handle_file(
                        filepath,
                        tar.extractfile(filepath).read().decode("utf-8")
                    )
        else:
            with LazyZipOverHTTP(url) as zf:
                with zipfile.ZipFile(zf) as zip_file:
                    for filepath in filepaths:
                        with zip_file.open(filepath) as file:
                            handle_file(filepath, file.read().decode("utf-8"))

        return result

    def close(self):
        self.client.close()
