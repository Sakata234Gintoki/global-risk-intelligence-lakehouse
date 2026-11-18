"""Delta Lake ACID upserts (MERGE) with change detection via a row hash."""
from __future__ import annotations

from collections.abc import Iterable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_HASH_EXCLUDE = ("_ingested_at", "_batch_id", "_source_file", "_row_hash")


def with_row_hash(df: DataFrame, exclude: Iterable[str]) -> DataFrame:
    """Add `_row_hash` = sha2 over all non-excluded columns (nulls normalised)."""
    skip = set(exclude)
    cols = sorted(c for c in df.columns if c not in skip)
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]
    return df.withColumn("_row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def latest_per_key(df: DataFrame, keys: list[str], order_col: str = "_ingested_at") -> DataFrame:
    from pyspark.sql import Window

    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def upsert_delta(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    keys: list[str],
    hash_exclude: Iterable[str] = DEFAULT_HASH_EXCLUDE,
) -> dict:
    """MERGE `df` into the Delta table at `path`.

    * matched + hash differs  -> UPDATE (unchanged rows are not rewritten)
    * not matched             -> INSERT
    Creates the table on first run. Returns Delta operation metrics.
    """
    source = with_row_hash(df, set(hash_exclude) | set(keys))

    if not DeltaTable.isDeltaTable(spark, path):
        source.write.format("delta").mode("overwrite").save(path)
        return {"numTargetRowsInserted": str(source.count()), "numTargetRowsUpdated": "0"}

    target = DeltaTable.forPath(spark, path)
    condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    (
        target.alias("t")
        .merge(source.alias("s"), condition)
        .whenMatchedUpdateAll(condition="t._row_hash <> s._row_hash")
        .whenNotMatchedInsertAll()
        .execute()
    )
    last = target.history(1).select("operationMetrics").first()
    return dict(last[0]) if last and last[0] else {}
