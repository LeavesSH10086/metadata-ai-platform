from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, when


def build_family_summary_df(normalized_df: DataFrame, 
                            dynamic_expansion_df: DataFrame):
    family_df = normalized_df.join(dynamic_expansion_df, 
                                   on=["cardnumber", "transnumber"], 
                                   how="inner"
                                   )
    summary_df = family_df.groupBy(
        "cardnumber",
        "root_purchase_transnumber",
    ).agg(
        spark_sum(
            when(
                (col("normalized_event") == "PURCHASE")
                & (col("transnumber") == col("root_purchase_transnumber")),
                1,
            ).otherwise(0)
        ).alias("root_purchase_count"),
        spark_sum(
            when(
                (col("normalized_event") == "PURCHASE")
                & (col("transnumber") != col("root_purchase_transnumber")),
                1,
            ).otherwise(0)
        ).alias("reposting_count"),
        spark_sum(
            when(col("normalized_event") == "RETURN", 1).otherwise(0)
        ).alias("return_count"),
        spark_sum(
            when(col("normalized_event") == "EXCHANGE", 1).otherwise(0)
        ).alias("exchange_count"),
        spark_sum(
            when(col("normalized_event") == "VOID_RETURN", 1).otherwise(0)
        ).alias("void_return_count"),
        spark_sum(
            when(col("normalized_event") == "VOID_EXCHANGE", 1).otherwise(0)
        ).alias("void_exchange_count"),
        spark_sum(
            when(
                col("normalized_event").isin("COMMIT_REDEEM", "REDEEM"),
                1,
            ).otherwise(0)
        ).alias("redeem_count"),
        spark_sum(col("loyaltycurrency")).alias("final_balance"),
    )

    return summary_df