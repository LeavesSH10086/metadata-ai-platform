from pathlib import Path
from pyspark.sql import SparkSession, DataFrame

from app.util.logger import LogMixin

logger = LogMixin().logger

def build_base_df(spark: SparkSession,
                  runtime_parameters: dict, 
                  hql_file_path: Path)->DataFrame: 
    """
    This function builds the base dataframe from lms_point detail for scenario detection engine.
    It filters the data based on the provided runtime and selects the necessary columns for further processing.
    filters: partition_batch_ts, banner, pointdate
    """

    with open(hql_file_path, "r", encoding="utf-8") as file:
        hql_query = file.read()

    required_columns = ",\n    ".join(runtime_parameters["required_columns"])
    query = (
        hql_query.replace("${partition_batch_ts}", f"'{runtime_parameters['partition_batch_ts']}'")
        .replace("${banner}", runtime_parameters["banner"])
        .replace("${date_col}", runtime_parameters.get("date_col", "pointdate"))
        .replace("${start_date}", str(runtime_parameters["start_date"]))
        .replace("${required_columns}", required_columns)
        .replace("${source_table}", runtime_parameters["source_table"])
    )

    logger.info("Executing HQL query to build base dataframe:\n%s", query)

    base_df = spark.sql(query)

    return base_df

