from app.util.logger import LogMixin
from pyspark.sql import SparkSession, DataFrame
from pathlib import Path

logger = LogMixin().logger

def build_normalized_df(spark: SparkSession, 
                        base_df: DataFrame, 
                        hql_file_path: Path) -> DataFrame:
    """
    This function normalizes the base dataframe by performing necessary transformations and cleaning.
    It prepares the data for further analysis in the lifecycle detection engine.
    """

    base_df.createOrReplaceTempView("base_extract")

    with open(hql_file_path, "r", encoding="utf-8") as file:
        query = file.read()

    logger.info("Executing HQL query to normalize events:\n%s", query)

    normalized_df = spark.sql(query)

    return normalized_df