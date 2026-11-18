"""Landing-zone manifests: written last by ingestion, used to find the newest batch."""
from __future__ import annotations

import json

import fsspec

from gril.config import Storage, to_fs_uri


def write_manifest(storage: Storage, batch_id: str, payload: dict) -> str:
    path = f"{storage.manifests()}/{batch_id}.json"
    uri = to_fs_uri(path)
    if "://" not in uri:
        import os

        os.makedirs(os.path.dirname(uri), exist_ok=True)
    with fsspec.open(uri, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    return path


def latest_batch_id(storage: Storage) -> str:
    fs, root = fsspec.core.url_to_fs(to_fs_uri(storage.manifests()))
    if not fs.exists(root):
        raise FileNotFoundError(f"No manifests found under {storage.manifests()} - run ingestion first.")
    names = sorted(p.rstrip("/").split("/")[-1] for p in fs.ls(root, detail=False))
    names = [n for n in names if n.endswith(".json")]
    if not names:
        raise FileNotFoundError(f"No manifests found under {storage.manifests()} - run ingestion first.")
    return names[-1][: -len(".json")]
