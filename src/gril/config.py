"""Central configuration: pipeline.yaml + storage layout helpers."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from importlib import resources
from pathlib import Path
from typing import Any

import yaml


@lru_cache(maxsize=1)
def load_config() -> dict[str, Any]:
    override = os.getenv("GRIL_CONFIG")
    if override:
        return yaml.safe_load(Path(override).read_text(encoding="utf-8"))
    return yaml.safe_load(resources.files("gril").joinpath("pipeline.yaml").read_text(encoding="utf-8"))


def to_spark_uri(path: str) -> str:
    """Spark/Hadoop wants s3a://, everything else wants s3://."""
    if path.startswith("s3://"):
        return "s3a://" + path[len("s3://"):]
    return path


def to_fs_uri(path: str) -> str:
    if path.startswith("s3a://"):
        return "s3://" + path[len("s3a://"):]
    return path


def new_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


@dataclass(frozen=True)
class Storage:
    """Lakehouse layout under one root (local folder, DBFS path or s3://bucket/prefix).

    <root>/landing/...        raw API payloads (JSON lines) + manifests
    <root>/bronze/<table>     Delta, raw + lineage columns
    <root>/silver/<table>     Delta, typed / deduped / validated
    <root>/gold/<table>       Delta, star schema + risk scores
    <root>/gold_export/<t>    Parquet snapshots consumed by Snowflake COPY INTO
    <root>/quality/...        Great Expectations result JSON
    """

    root: str

    @classmethod
    def from_env(cls) -> Storage:
        root = os.getenv("GRIL_STORAGE_ROOT", "./data")
        if "://" not in root and not root.startswith("/dbfs"):
            root = os.path.abspath(root)
        return cls(root=root.rstrip("/"))

    @property
    def is_s3(self) -> bool:
        return self.root.startswith(("s3://", "s3a://"))

    def _p(self, *parts: str) -> str:
        return "/".join([self.root, *parts])

    def landing(self, *parts: str) -> str:
        return self._p("landing", *parts)

    def manifests(self) -> str:
        return self._p("landing", "_manifests")

    def bronze(self, table: str) -> str:
        return self._p("bronze", table)

    def silver(self, table: str) -> str:
        return self._p("silver", table)

    def gold(self, table: str) -> str:
        return self._p("gold", table)

    def gold_export(self, table: str) -> str:
        return self._p("gold_export", table)

    def quality(self, *parts: str) -> str:
        return self._p("quality", *parts)
