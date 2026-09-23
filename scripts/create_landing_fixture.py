"""Convert the synthetic demo array to Auto Loader newline-delimited JSON."""
import json
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("output", type=Path)
args = parser.parse_args()
source = Path(__file__).resolve().parents[1] / "data" / "synthetic_claims.json"
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text("".join(json.dumps(row) + "\n" for row in json.loads(source.read_text())), encoding="utf-8")
print(f"Wrote synthetic JSONL to {args.output}")
