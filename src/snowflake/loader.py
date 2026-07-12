"""Writes curated micro-batches to Snowflake using the Spark Snowflake connector."""

from pyspark.sql import DataFrame


def write_to_snowflake(
      batch_df: DataFrame,
      batch_id: int,
      options: dict,
      table: str = "RETAIL_SALES_CURATED",
) -> None:
      """Writes a micro-batch to a Snowflake table; safe to retry on failure since
          downstream aggregation keys are idempotent per store, sku, and event date."""
      row_count = batch_df.count()
      (
          batch_df.write.format("net.snowflake.spark.snowflake")
          .options(**options)
          .option("dbtable", table)
          .mode("append")
          .save()
      )
      print(f"[batch {batch_id}] wrote {row_count} rows to {table}")
  
