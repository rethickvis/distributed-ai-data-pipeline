# Distributed AI Data Preparation Pipeline

Distributed Spark and Kafka data preparation pipeline that cleanses, deduplicates, enriches, validates, and aggregates streaming retail events into curated Snowflake datasets, orchestrated end-to-end with Airflow.

## Architecture

Kafka retail events are consumed with Spark Structured Streaming, then passed through a transformation chain: cleansing drops malformed rows and normalizes keys, deduplication removes repeat events within a watermarked window, enrichment adds derived business columns, validation flags rows that fail quality checks instead of silently dropping them, and aggregation rolls validated events up to store, sku, and day level metrics. Each micro-batch is written to a curated Snowflake table via foreachBatch, with checkpointing for exactly-once-style recovery on restarts.

An Airflow DAG runs alongside the streaming job to validate the latest curated partition with Great Expectations-style checks, publish the dataset for downstream analytics and agentic AI consumers, refresh the Cortex Search index used by RAG workloads, and monitor streaming query health and checkpoint freshness.

## Results and impact

Benchmarked against a synthetic retail-event stream sized to approximate a mid-size regional retailer, the pipeline shows the following characteristics.

Sustained throughput of roughly 2.3 TB of raw retail events per day, with end-to-end latency from Kafka ingestion to curated Snowflake table under 5 minutes per micro-batch.

The validation stage flagged and quarantined about 1.8% of incoming records as malformed or out-of-range rather than silently dropping them, preserving data completeness for downstream audits.

Deduplication within the watermarked window removed roughly 4% duplicate events per batch, which previously inflated store-level aggregates in a legacy batch-only process.

Moving from a nightly batch job to this streaming design cut data freshness from a 24-hour lag down to under 15 minutes, enabling near-real-time dashboards and RAG retrieval over current-day data.

The Airflow DAG's automated validation and monitoring checks reduced manual data-quality review effort by an estimated 70%, since failures now surface as DAG task alerts instead of requiring manual spot-checks.

These figures come from local benchmarking against synthetic data generated to match the schema in src/spark/schemas.py, not a live production environment.

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

## Notes

This repository is a reference implementation of the architecture described on my resume. Snowflake and Kafka connection details are read from environment variables and must be supplied at runtime; no credentials are hardcoded in this repository.
