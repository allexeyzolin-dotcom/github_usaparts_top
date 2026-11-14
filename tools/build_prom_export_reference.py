"""Build a sanitized Prom metadata reference; private notes are never retained."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.prom_export import read_reference

parser = argparse.ArgumentParser()
parser.add_argument('source', type=Path)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
reference = read_reference(args.source.read_bytes(), args.source.name)
args.output.write_text(json.dumps(reference, ensure_ascii=False, separators=(',', ':')) + '\n', encoding='utf-8')
print(f"Reference products: {len(reference['records'])}; bytes: {args.output.stat().st_size}")
