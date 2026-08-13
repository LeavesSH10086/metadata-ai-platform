from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper
from app.util.logger import LogMixin

logger = LogMixin().logger


def build_recursive_graph_expansion_df(normalized_df: DataFrame) -> DataFrame:
    """
    This function performs recursive graph expansion on the normalized dataframe.
    It iteratively expands the graph based on the defined relationships until no further expansion is possible.
    """

    root_purchase_df = (
        normalized_df.filter(
            (upper(trim(col("normalized_event"))) == "PURCHASE")
            & (col("transnumber") == col("originating_transnumber"))
        )
        .select(
            col("transnumber").alias("root_purchase_transnumber"),
            "cardnumber",
        )
        .distinct()
    )
    known_df = root_purchase_df.select(
        "cardnumber",
        "root_purchase_transnumber",
        col("root_purchase_transnumber").alias("transnumber"),
    ).distinct()

    previous_count = 0
    current_count = known_df.count()

    while current_count != previous_count:
        previous_count = current_count
        expanded_df = (
            known_df.alias("known")
            .join(
                normalized_df.alias("normalized"),
                (col("known.cardnumber") == col("normalized.cardnumber"))
                & (
                    (
                        col("known.transnumber")
                        == col("normalized.original_transaction_num")
                    )
                    | (
                        col("known.transnumber")
                        == col("normalized.originating_transnumber")
                    )
                ),
                "inner",
            )
            .select(
                col("normalized.cardnumber"),
                col("known.root_purchase_transnumber"),
                col("normalized.transnumber"),
            )
            .distinct()
        )
        known_df = known_df.unionByName(expanded_df).distinct()
        current_count = known_df.count()
        logger.info(
            "Recursive graph expansion count: %s -> %s",
            previous_count,
            current_count,
        )

    return known_df
   
