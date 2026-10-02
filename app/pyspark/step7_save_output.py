from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def save_output(
                output_df: DataFrame,
                output_path: str,
                output_schema: list[dict],
                output_mode: str = "append",
            ) -> None:
    if not output_path:
        raise ValueError("An output path is required")
    if not output_schema:
        raise ValueError("The output schema must define at least one column")

    schema_columns = [field["name"] for field in output_schema]
    if len(schema_columns) != len(set(schema_columns)):
        raise ValueError("The output schema contains duplicate column names")

    missing_columns = sorted(set(schema_columns) - set(output_df.columns))
    if missing_columns:
        raise ValueError("Output DataFrame is missing schema columns: " + ", ".join(missing_columns))

    schema_df = output_df.select(*[col(field["name"]).cast(field["type"]).alias(field["name"]) for field in output_schema])

    required_columns = [field["name"] for field in output_schema if not field.get("nullable", True)]
    if required_columns:
        null_condition = col(required_columns[0]).isNull()
        for column_name in required_columns[1:]:
            null_condition = null_condition | col(column_name).isNull()

        if schema_df.filter(null_condition).limit(1).count():
            raise ValueError("Output DataFrame contains null values in required columns: " + ", ".join(required_columns))

    schema_df.write.mode(output_mode).parquet(output_path)