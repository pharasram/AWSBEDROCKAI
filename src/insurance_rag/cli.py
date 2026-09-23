import argparse
import json
from pathlib import Path
from .core import LocalStore


def main():
    parser = argparse.ArgumentParser(description="Insurance RAG offline demo")
    parser.add_argument("--db", default="insurance.db")
    commands = parser.add_subparsers(dest="command", required=True)
    ingest = commands.add_parser("ingest")
    ingest.add_argument("path", type=Path)
    ingest.add_argument("--expected-count", required=True, type=int)
    ingest.add_argument("--batch-id", required=True)
    query = commands.add_parser("query")
    query.add_argument("question")
    query.add_argument("--product")
    query.add_argument("--top-k", type=int, default=3)
    args = parser.parse_args()
    store = LocalStore(args.db)
    try:
        if args.command == "ingest":
            rows = json.loads(args.path.read_text(encoding="utf-8"))
            print(json.dumps({"staged": store.ingest(rows, args.expected_count, args.batch_id), "status": "SUCCEEDED"}))
        else:
            print(json.dumps(store.query(args.question, args.top_k, args.product), indent=2))
    finally:
        store.close()


if __name__ == "__main__":
    main()
