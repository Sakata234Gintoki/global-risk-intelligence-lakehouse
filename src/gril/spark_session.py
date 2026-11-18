"""SparkSession factory: Databricks-native or local Spark + Delta (+ S3A)."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from gril.config import Storage


def get_spark(app_name: str = "gril") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", os.getenv("GRIL_SHUFFLE_PARTITIONS", "8"))
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )

    # On Databricks the runtime already ships Delta + S3 connectors.
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    extra_packages: list[str] = []
    if Storage.from_env().is_s3:
        extra_packages.append("org.apache.hadoop:hadoop-aws:3.3.4")
        builder = (
            builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
        )
    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
