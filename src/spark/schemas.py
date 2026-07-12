"""Schema definitions for streaming Kafka events processed by the pipeline."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RETAIL_EVENT_SCHEMA = StructType([
      StructField("event_id", StringType(), nullable=False),
      StructField("store_id", StringType(), nullable=False),
      StructField("sku", StringType(), nullable=False),
      StructField("quantity", IntegerType(), nullable=False),
      StructField("unit_price", DoubleType(), nullable=False),
      StructField("event_type", StringType(), nullable=False),
      StructField("event_timestamp", TimestampType(), nullable=False),
      StructField("source_system", StringType(), nullable=True),
])
