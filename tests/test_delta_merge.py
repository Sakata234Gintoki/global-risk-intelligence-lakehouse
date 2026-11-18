from gril.utils.delta_merge import upsert_delta


def _read(spark, path):
    return {r.id: r.v for r in spark.read.format("delta").load(path).collect()}


def test_merge_is_idempotent_and_updates_changed_rows(spark, tmp_path):
    path = str(tmp_path / "tbl")

    upsert_delta(spark, spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"]), path, ["id"])
    assert _read(spark, path) == {1: "a", 2: "b"}

    # re-applying identical data changes nothing
    upsert_delta(spark, spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"]), path, ["id"])
    assert _read(spark, path) == {1: "a", 2: "b"}

    # one update + one insert, untouched row kept
    metrics = upsert_delta(spark, spark.createDataFrame([(2, "B"), (3, "c")], ["id", "v"]), path, ["id"])
    assert _read(spark, path) == {1: "a", 2: "B", 3: "c"}
    assert metrics.get("numTargetRowsUpdated") == "1"
    assert metrics.get("numTargetRowsInserted") == "1"
