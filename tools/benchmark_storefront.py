"""Comparable cold/warm Flask render timing; no writes with crawler requests."""
import json
import statistics
import time
from sqlalchemy import event
import app.app as site

queries = []


@event.listens_for(site.engine, "before_cursor_execute")
def record_query(*args):
    queries.append(1)


client = site.app.test_client()
result = []
for path in ["/", "/catalog", "/catalog?q=09P321361", "/sitemap/images.xml"]:
    samples, query_counts = [], []
    for iteration in range(8):
        queries.clear()
        start = time.perf_counter()
        response = client.get(path, headers={"User-Agent": "Googlebot", "Accept-Encoding": "gzip"})
        samples.append(round((time.perf_counter() - start) * 1000, 2))
        query_counts.append(len(queries))
        assert response.status_code == 200, (path, response.status_code)
    result.append({"path": path, "first_ms": samples[0], "warm_median_ms": statistics.median(samples[1:]),
                   "warm_sql_queries": statistics.median(query_counts[1:]), "response_bytes": len(response.data),
                   "content_encoding": response.headers.get("Content-Encoding", "identity")})
print(json.dumps(result, indent=2))
