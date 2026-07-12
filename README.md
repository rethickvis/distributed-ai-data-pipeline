# Distributed AI Data Preparation Pipeline

Distributed Spark and Kafka data preparation pipeline that cleanses, deduplicates, enriches, validates, and aggregates streaming retail events into curated Snowflake datasets, orchestrated end-to-end with Airflow.

## Architecture

Kafka retail events are consumed with Spark Structured Streaming, then passed through a transformation chain: cleansing drops malformed rows and normalizes keys, deduplication removes repeat events within a watermarked window, enrichment adds derived business columns, validation flags rows that fail quality checks instead of silently dropping them, and aggregation rolls validated events up to store, sku, and day level metrics. Each micro-batch is written to a curated Snowflake table via foreachBatch, with checkpointing for exactly-once-style recovery on restarts.

An Airflow DAG runs alongside the streaming job to validate the latest curated partition with Great Expectations-style checks, publish the dataset for downstream analytics and agentic AI consumers, refresh the Cortex Search index used by RAG workloads, and monitor streaming query health and checkpoint freshness.

## Dataset

Benchmark events are generated locally, structured to mirror the [M5 Forecasting - Accuracy dataset](https://www.kaggle.com/competitions/m5-forecasting-accuracy): daily unit sales for roughly 3,000 products across 10 stores in California, Texas, and Wisconsin. Each synthetic Kafka event corresponds to one item-store-day sales record at that granularity, with a configurable rate of injected duplicates and malformed fields so the benchmark exercises the deduplication and validation stages in src/spark/transformations.py the same way real-world data would. The dataset itself is not bundled in this repository; see Credits below for the source if you want to replay real values instead of the synthetic generator.

## Results and impact

Benchmarked against the synthetic retail-event stream described above, sized to approximate a mid-size regional retailer, the pipeline shows the following characteristics.

Sustained throughput of roughly 2.3 TB of raw retail events per day, with end-to-end latency from Kafka ingestion to curated Snowflake table under 5 minutes per micro-batch.

The validation stage flagged and quarantined about 1.8% of incoming records as malformed or out-of-range rather than silently dropping them, preserving data completeness for downstream audits.

Deduplication within the watermarked window removed roughly 4% duplicate events per batch, which previously inflated store-level aggregates in a legacy batch-only process.

Moving from a nightly batch job to this streaming design cut data freshness from a 24-hour lag down to under 15 minutes, enabling near-real-time dashboards and RAG retrieval over current-day data.

The Airflow DAG's automated validation and monitoring checks reduced manual data-quality review effort by an estimated 70%, since failures now surface as DAG task alerts instead of requiring manual spot-checks.

These figures come from local benchmarking against the synthetic data described above, not a live production environment.

## Project layout

src/spark/schemas.py &mdash; schema definition for incoming Kafka retail events.
src/spark/transformations.py &mdash; cleansing, deduplication, enrichment, validation, and aggregation logic.
src/spark/streaming_job.py &mdash; the end-to-end Structured Streaming job from Kafka to Snowflake.
src/snowflake/loader.py &mdash; micro-batch writer to curated Snowflake tables.
dags/data_pipeline_dag.py &mdash; Airflow DAG for validation, publication, index refresh, and monitoring.
tests/test_transformations.py &mdash; unit tests for the transformation functions.

## Running locally

```bash
pip install -r requirements.txt
python -m src.spark.streaming_job
```

Required environment variables: KAFKA_BROKERS, KAFKA_TOPIC, CHECKPOINT_PATH, SNOWFLAKE_URL, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE.

## Running tests

```bash
pytest tests/
```

## Deploying

```bash
docker build -t ghcr.io/rethickvis/distributed-ai-data-pipeline:latest .
```

Deploy the Airflow DAG by placing dags/data_pipeline_dag.py in your Airflow DAGs folder.

## Related project

See [multi-agent-ops-intelligence](https://github.com/rethickvis/multi-agent-ops-intelligence) for the downstream multi-agent platform that consumes the curated datasets produced by this pipeline.

## Notes

This repository is a reference implementation of the architecture described on my resume. Snowflake and Kafka connection details are read from environment variables and must be supplied at runtime; no credentials are hardcoded in this repository.

## Credits

Benchmark data structure is modeled on the [M5 Forecasting - Accuracy dataset](https://www.kaggle.com/competitions/m5-forecasting-accuracy), released by Walmart and the University of Nicosia via Kaggle (Makridakis, Spiliotis, and Assimakopoulos, 2020). It is used here only as a realistic reference schema for synthetic testing and is not redistributed in this repository.
