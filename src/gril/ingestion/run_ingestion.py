"""Pull WHO GHO + World Bank data into the landing zone as JSON lines.

Writes one file per (source, indicator, batch) and a manifest LAST. The manifest is the
"event" the downstream Airflow DAG is triggered by (Dataset-aware scheduling).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from collections.abc import Iterable
from datetime import datetime, timezone

import fsspec

from gril.config import Storage, load_config, new_batch_id, to_fs_uri
from gril.ingestion.clients import (
    build_session,
    fetch_wb_countries,
    fetch_wb_indicator,
    fetch_who_indicator,
    flatten_wb_country,
    flatten_wb_observation,
    flatten_who_record,
)
from gril.utils.manifest import write_manifest

log = logging.getLogger("gril.ingestion")


def write_jsonl(path: str, rows: Iterable[dict]) -> int:
    uri = to_fs_uri(path)
    if "://" not in uri:
        os.makedirs(os.path.dirname(uri), exist_ok=True)
    n = 0
    with fsspec.open(uri, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
            n += 1
    return n


def run(batch_id: str | None = None) -> str:
    cfg = load_config()
    storage = Storage.from_env()
    batch_id = batch_id or new_batch_id()
    y0, y1 = cfg["year_range"]["start"], cfg["year_range"]["end"]
    session = build_session()
    files: list[dict] = []

    for ind in cfg["who_indicators"]:
        code = ind["code"]
        rows = [flatten_who_record(r, code) for r in fetch_who_indicator(session, code, y0, y1)]
        path = storage.landing("who", code, f"batch_id={batch_id}", "data.jsonl")
        files.append({"source": "who", "indicator": code, "path": path, "rows": write_jsonl(path, rows)})
        log.info("WHO %s -> %d rows", code, len(rows))

    countries = [flatten_wb_country(r) for r in fetch_wb_countries(session)]
    path = storage.landing("worldbank", "countries", f"batch_id={batch_id}", "data.jsonl")
    files.append({"source": "worldbank", "indicator": "countries", "path": path, "rows": write_jsonl(path, countries)})
    log.info("WB countries -> %d rows", len(countries))

    for ind in cfg["worldbank_indicators"]:
        code = ind["code"]
        rows = [flatten_wb_observation(r) for r in fetch_wb_indicator(session, code, y0, y1)]
        path = storage.landing("worldbank", "indicators", code, f"batch_id={batch_id}", "data.jsonl")
        files.append({"source": "worldbank", "indicator": code, "path": path, "rows": write_jsonl(path, rows)})
        log.info("WB %s -> %d rows", code, len(rows))

    empty = [f for f in files if f["rows"] == 0]
    if empty:
        raise RuntimeError(f"Ingestion produced empty extracts, refusing to publish manifest: {empty}")

    write_manifest(
        storage,
        batch_id,
        {
            "batch_id": batch_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "files": files,
            "total_rows": sum(f["rows"] for f in files),
        },
    )
    log.info("Batch %s complete (%d files)", batch_id, len(files))
    return batch_id


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Ingest WHO + World Bank data into the landing zone")
    parser.add_argument("--batch-id", default=None)
    args = parser.parse_args()
    run(args.batch_id)


if __name__ == "__main__":
    main()
