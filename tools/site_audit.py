"""Read-only sitemap/canonical audit. Use --sample 0 for every sitemap URL."""
import argparse
import json
import time
from collections import Counter
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit
import xml.etree.ElementTree as ET

import requests

NS = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class Page(HTMLParser):
    def __init__(self):
        super().__init__()
        self.canonicals, self.links, self.images, self.schemas = [], [], [], []
        self.robots, self.title, self.description = "", "", ""
        self.h1 = 0
        self.in_title = self.in_schema = False
        self.schema = ""

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "link" and a.get("rel") == "canonical":
            self.canonicals.append(a.get("href", ""))
        if tag == "meta" and a.get("name") == "robots":
            self.robots = a.get("content", "")
        if tag == "meta" and a.get("name") == "description":
            self.description = a.get("content", "")
        if tag == "a" and a.get("href"):
            self.links.append(a["href"])
        if tag == "img":
            self.images.append(a.get("src", ""))
        if tag == "h1":
            self.h1 += 1
        if tag == "title":
            self.in_title = True
        if tag == "script" and a.get("type") == "application/ld+json":
            self.in_schema, self.schema = True, ""

    def handle_data(self, data):
        if self.in_title:
            self.title += data
        if self.in_schema:
            self.schema += data

    def handle_endtag(self, tag):
        if tag == "title":
            self.in_title = False
        if tag == "script" and self.in_schema:
            self.schemas.append(self.schema)
            self.in_schema = False


def run(base, sample, delay):
    client = requests.Session()
    client.headers["User-Agent"] = "USAparts-SiteAudit/1.0 (technical site check)"
    report = {"base": base, "sitemaps": [], "pages": [], "issues": []}

    def target(url):
        parsed = urlsplit(url)
        return base.rstrip("/") + parsed.path + ("?" + parsed.query if parsed.query else "")

    urls = set()
    pending = [base.rstrip("/") + "/sitemap.xml"]
    visited = set()
    while pending:
        url = pending.pop(0)
        if url in visited:
            continue
        visited.add(url)
        try:
            start = time.monotonic()
            r = client.get(target(url), timeout=(10, 30), allow_redirects=False)
            root = ET.fromstring(r.content)
            is_index = root.tag == "{" + NS["s"] + "}sitemapindex"
            locs = [n.text for n in root.findall("s:sitemap/s:loc" if is_index else "s:url/s:loc", NS)]
            report["sitemaps"].append({"url": url, "status": r.status_code, "count": len(locs), "seconds": round(time.monotonic() - start, 3)})
            if r.status_code != 200 or not locs or "xml" not in r.headers.get("Content-Type", ""):
                report["issues"].append({"url": url, "issue": "Invalid sitemap response"})
            if is_index:
                pending.extend(locs)
            else:
                urls.update(locs)
        except (requests.RequestException, ET.ParseError) as exc:
            report["issues"].append({"url": url, "issue": type(exc).__name__})
        time.sleep(delay)

    selected = sorted(urls)
    report["unique_sitemap_urls"] = len(selected)
    if sample and len(selected) > sample:
        # Evenly spread across the entire index, not just the first products.
        selected = [selected[i * (len(selected) - 1) // (sample - 1)] for i in range(sample)] if sample > 1 else selected[:1]
    for url in selected:
        start = time.monotonic()
        row = {"url": url, "issues": []}
        try:
            r = client.get(target(url), timeout=(10, 30), allow_redirects=False)
            row.update(status=r.status_code, seconds=round(time.monotonic() - start, 3), bytes=len(r.content))
            if r.status_code != 200:
                row["issues"].append("HTTP " + str(r.status_code))
            else:
                page = Page()
                page.feed(r.text)
                row["title"] = page.title
                row["canonical"] = page.canonicals
                if page.canonicals != [url]:
                    row["issues"].append("Canonical differs from sitemap URL")
                if "noindex" in (page.robots + r.headers.get("X-Robots-Tag", "")).lower():
                    row["issues"].append("Noindex in sitemap")
                if not page.title or not page.description or page.h1 != 1:
                    row["issues"].append("Missing title/description or H1 count != 1")
                for schema in page.schemas:
                    try:
                        data = json.loads(schema)
                        for product in data.get("@graph", [data]):
                            if product.get("@type") != "Product":
                                continue
                            if not product.get("sku") or product.get("offers", {}).get("priceCurrency") != "UAH":
                                row["issues"].append("Invalid product identifiers/currency")
                            if any("placeholder" in image for image in product.get("image", [])):
                                row["issues"].append("Placeholder used as product image")
                    except (ValueError, TypeError):
                        row["issues"].append("Invalid JSON-LD")
        except requests.RequestException as exc:
            row["issues"].append(type(exc).__name__)
        report["pages"].append(row)
        time.sleep(delay)
    report["checked_pages"] = len(report["pages"])
    report["page_issue_counts"] = dict(Counter(issue for row in report["pages"] for issue in row["issues"]))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://localhost:8080")
    parser.add_argument("--sample", type=int, default=60)
    parser.add_argument("--delay", type=float, default=0.15)
    parser.add_argument("--output", default="/tmp/site-audit.json")
    args = parser.parse_args()
    result = run(args.base, args.sample, args.delay)
    Path(args.output).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k != "pages"}, ensure_ascii=False, indent=2))
