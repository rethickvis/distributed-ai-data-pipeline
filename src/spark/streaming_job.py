"""Structured Streaming job: Kafka to cleansed, enriched, validated, aggregated
curated Snowflake tables."""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.snowflake.loader import write_to_snowflake
from src.spark.schemas import RETAIL_EVENT_SCHEMA
from src.spark.transformations import aggregate_sales, cleanse, deduplicate, enrich, validate


def build_spark_session(app_name: str = "distributed-ai-data-pipeline") -> SparkSession:
      return (
                SparkSession.builder.appName(app_name)
                .config("spark.sql.shuffle.partitions", "200")
                .config(
                              "spark.sql.streaming.stateStore.providerClass",
                              "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
                )
                .getOrCreate()
      )


def read_kafka_stream(spark: SparkSession, brokers: str, topic: str):
      raw = (
                spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("subscribe", topic)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
      )
      return raw.select(
          F.from_json(F.col("value").cast("string"), RETAIL_EVENT_SCHEMA).alias("event")
      ).select("event.*")


def run(
      brokers: str,
      topic: str,
      checkpoint_path: str,
      snowflake_options: dict,
      trigger_interval: str = "30 seconds",
):
      """Runs the end-to-end streaming pipeline from Kafka to curated Snowflake tables."""
      spark = build_spark_session()

    events = read_kafka_stream(spark, brokers, topic)
    cleansed = cleanse(events)
    deduped = deduplicate(cleansed)
    enriched = enrich(deduped)
    validated = validate(enriched)
    aggregated = aggregate_sales(validated)

    query = (
              aggregated.writeStream.foreachBatch(
                            lambda batch_df, batch_id: write_to_snowflake(batch_df, batch_id, snowflake_options)
              )
              .option("checkpointLocation", checkpoint_path)
              .trigger(processingTime=trigger_interval)
              .outputMode("update")
              .start()
    )
    return query


if __name__ == "__main__":
      run(
                brokers=os.environ["KAFKA_BROKERS"],
                topic=os.environ.get("KAFKA_TOPIC", "retail_events"),
                checkpoint_path=os.environ.get("CHECKPOINT_PATH", "/tmp/checkpoints/retail_events"),
                snowflake_options={
                              "sfURL": os.environ["SNOWFLAKE_URL"],
                              "sfDatabase": os.environ.get("SNOWFLAKE_DATABASE", "OPS_INTELLIGENCE"),
                              "sfSchema": os.environ.get("SNOWFLAKE_SCHEMA", "CURATED"),
                              "sfWarehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "OPS_WH"),
                              "sfRole": os.environ.get("SNOWFLAKE_ROLE", "OPS_LOADER"),
                },
      )
  
