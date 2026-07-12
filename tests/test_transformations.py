"""Unit tests for cleansing, enrichment, validation, and aggregation transforms."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.spark.transformations import cleanse, enrich, validate


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[2]").appName("test-transformations").getOrCreate()
    yield session
    session.stop()


def test_cleanse_drops_invalid_rows(spark):
    df = spark.createDataFrame(
        [
            ("e1", " s1 ", " sku1 ", 2, 10.0),
            ("e2", "s2", "sku2", -1, 5.0),
            (None, "s3", "sku3", 1, 5.0),
        ],
        ["event_id", "store_id", "sku", "quantity", "unit_price"],
    )
    result = cleanse(df)
    assert result.count() == 1
    row = result.collect()[0]
    assert row["store_id"] == "S1"
    assert row["sku"] == "SKU1"


def test_enrich_adds_gross_amount(spark):
    df = spark.createDataFrame(
        [("e1", "s1", "sku1", 2, 10.0, "2026-01-01 00:00:00")],
        ["event_id", "store_id", "sku", "quantity", "unit_price", "event_timestamp"],
    ).withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    enriched = enrich(df)
    assert "gross_amount" in enriched.columns
    assert enriched.collect()[0]["gross_amount"] == 20.0


def test_validate_flags_invalid_rows(spark):
    df = spark.createDataFrame(
        [
            ("e1", "s1", "sku1", 2, 10.0),
            ("e2", None, "sku2", 1, 5.0),
        ],
        ["event_id", "store_id", "sku", "quantity", "unit_price"],
    )
    result = validate(df)
    valid_flags = [row["is_valid"] for row in result.collect()]
    assert valid_flags == [True, False]
