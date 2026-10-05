# Lifecycle Detection Engine Testing Strategy

## Purpose

Run the complete lifecycle detection pipeline against approximately 15 days of
production-like transaction data for one banner. The test verifies extraction,
normalization, transaction-family expansion, lifecycle classification, final
schema validation, and output persistence.

The pipeline is orchestrated by `app/run_engine.py`. Do not run the individual
step modules manually.

## Prerequisites

- Python 3.11 or 3.12 with the project dependencies installed.
- A working PySpark environment with Hive support.
- Read access to `epsilon_lms_encrypted_raw.lms_point_detail`.
- Hadoop configuration and write access to the configured HDFS output path.
- Run commands from the `metadata-ai-platform` project root.

## Test Configuration

Edit `app/metadata/runtime_parameters.yaml` before each test run:

- `partition_batch_ts`: Set the earliest source partition included in the test.
- `start_date`: Set the earliest transaction date included in the test. A
    15-day lookback is normally sufficient.
- `banner`: Set the banner under test. The default test banner is `CTR`.
- `output_path`: Set the HDFS destination for test output.
- `output_mode`: Use `append` only when retaining previous output is intended.
    Use `overwrite` for an isolated repeatable test destination.

Confirm that `app/metadata/output_schema.yaml` contains the expected output
columns and types. It is an output contract and does not contain runtime date or
banner parameters.

## Run The Engine

From the `metadata-ai-platform` project root, run:

```bash
python -m app.run_engine
```

Use the environment's standard `spark-submit` wrapper instead when required by
the target Spark cluster. The wrapper must preserve the project root on
`PYTHONPATH` so imports from `app` resolve correctly.

One engine invocation runs the following stages in order:

1. Extract the configured date range and banner from the LMS point-detail table.
2. Normalize source transaction events and relationship fields.
3. Recursively expand transaction relationships into purchase families.
4. Build event-level family data and aggregate lifecycle metrics.
5. Classify each family using the configured scenario rules.
6. Build the final output DataFrame, including the pipeline operation timestamp.
7. Cast and order columns using `output_schema.yaml`, validate required fields,
     and write the result to the configured HDFS path.

## Validation

The test passes when all of the following are true:

- The engine completes without a Spark analysis or Python exception.
- The logged sample output contains the configured banner only.
- Every output row has non-null `scenario_name`, `banner`,
    `root_purchase_transnumber`, and `transnumber` values.
- Output columns and types match `app/metadata/output_schema.yaml`.
- `pipelne_operation_ts` is populated with the current pipeline execution time.
- The expected Parquet files are present under `output_path`.

For an append-mode test, verify that rerunning the same date range does not
create unintended duplicate business rows. Use a dedicated overwrite path when
testing pipeline behavior rather than append semantics.