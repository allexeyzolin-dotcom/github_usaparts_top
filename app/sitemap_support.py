"""Split UTF-8 sitemaps by both URL count and uncompressed byte size."""

SITEMAP_MAX_URLS = 10_000
SITEMAP_MAX_BYTES = 45 * 1024 * 1024
SITEMAP_NAMESPACE = "http://www.sitemaps.org/schemas/sitemap/0.9"
IMAGE_NAMESPACE = "http://www.google.com/schemas/sitemap-image/1.1"


def sitemap_documents(nodes, *, images=False, max_urls=None, max_bytes=None):
    max_urls = SITEMAP_MAX_URLS if max_urls is None else max_urls
    max_bytes = SITEMAP_MAX_BYTES if max_bytes is None else max_bytes
    if not 1 <= max_urls <= 50_000 or not 1 <= max_bytes <= 50 * 1024 * 1024:
        raise ValueError("Invalid sitemap limits")
    image_namespace = f' xmlns:image="{IMAGE_NAMESPACE}"' if images else ""
    header = ('<?xml version="1.0" encoding="UTF-8"?>\n'
              f'<urlset xmlns="{SITEMAP_NAMESPACE}"{image_namespace}>\n')
    footer = "</urlset>\n"
    overhead = len((header + footer).encode("utf-8"))
    if overhead > max_bytes:
        raise ValueError("Sitemap size limit is smaller than the XML wrapper")
    documents, chunk = [], []
    size = overhead
    for node in nodes:
        node_size = len((node + "\n").encode("utf-8"))
        if overhead + node_size > max_bytes:
            raise ValueError("A sitemap entry exceeds the uncompressed size limit")
        if chunk and (len(chunk) >= max_urls or size + node_size > max_bytes):
            documents.append(header + "\n".join(chunk) + "\n" + footer)
            chunk, size = [], overhead
        chunk.append(node)
        size += node_size
    if chunk or not documents:
        documents.append(header + "\n".join(chunk) + ("\n" if chunk else "") + footer)
    return documents
