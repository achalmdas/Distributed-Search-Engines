"""Check the sample dataset before starting services."""
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    data_file = root / "index_node" / "data" / "documents.json"

    try:
        documents = json.loads(data_file.read_text(encoding="utf-8"))

        if not isinstance(documents, list) or not documents:
            raise ValueError("Dataset must be a non-empty JSON list")

        seen_ids = set()
        shard_counts = [0, 0, 0]

        for position, document in enumerate(documents, start=1):
            if not isinstance(document, dict):
                raise ValueError(
                    f"Document {position} must be a JSON object"
                )

            doc_id = document.get("doc_id")

            if type(doc_id) is not int or doc_id < 0:
                raise ValueError(
                    f"Document {position} needs a non-negative integer doc_id"
                )

            if doc_id in seen_ids:
                raise ValueError(f"Duplicate doc_id: {doc_id}")

            for field in ("title", "body"):
                value = document.get(field)
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(
                        f"Document {doc_id} needs a non-empty {field}"
                    )

            seen_ids.add(doc_id)
            shard_counts[doc_id % 3] += 1

    except (OSError, UnicodeError, ValueError) as error:
        print(f"FAIL: {error}")
        return 1

    print(f"PASS: {len(documents)} valid documents with unique IDs")
    for shard, count in enumerate(shard_counts):
        print(f"Shard {shard} (port {8001 + shard}): {count} documents")

    print("ALL DATASET CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())