"""Cleansing, deduplication, enrichment, validation, and aggregation transforms."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def cleanse(df: DataFrame) -> DataFrame:
      """Drops malformed rows and normalizes key columns."""
      return (
          df.filter(F.col("event_id").isNotNull())
          .filter(F.col("quantity") > 0)
          .filter(F.col("unit_price") >= 0)
          .withColumn("sku", F.upper(F.trim(F.col("sku"))))
          .withColumn("store_id", F.upper(F.trim(F.col("store_id"))))
      )


def deduplicate(df: DataFrame, watermark_col: str = "event_timestamp") -> DataFrame:
      """Deduplicates events by event_id within a watermarked window."""
      return df.withWatermark(watermark_col, "10 minutes").dropDuplicates(["event_id"])


def enrich(df: DataFrame) -> DataFrame:
      """Adds derived columns used by downstream analytics and agentic AI workloads."""
      return df.withColumn(
          "gross_amount", F.col("quantity") * F.col("unit_price")
      ).withColumn("event_date", F.to_date(F.col("event_timestamp")))


def validate(df: DataFrame) -> DataFrame:
      """Flags rows that fail basic quality checks instead of silently dropping them."""
      return df.withColumn(
          "is_valid",
          F.col("quantity").isNotNull()
          & F.col("unit_price").isNotNull()
          & F.col("sku").isNotNull()
          & F.col("store_id").isNotNull(),
      )


def aggregate_sales(df: DataFrame) -> DataFrame:
      """Aggregates validated events into store, sku, and day level sales metrics."""
      return (
          df.filter(F.col("is_valid"))
          .groupBy("store_id", "sku", "event_date")
          .agg(
              F.sum("gross_amount").alias("gross_sales"),
              F.sum("quantity").alias("units_sold"),
              F.count("event_id").alias("transaction_count"),
          )
      )
  
