from pyspark.sql import DataFrame
from pyspark.sql.functions import col, substring, trim, upper


def build_family_event_df(
    normalized_df: DataFrame,
    dynamic_expansion_df: DataFrame,
) -> DataFrame:
    event_df = normalized_df.select(trim(col("cardnumber")).alias("cardnumber"),
                                    trim(col("transnumber")).alias("transnumber"),
                                    trim(col("original_transaction_num")).alias(
                                        "original_transaction_num"
                                    ),
                                    upper(trim(col("normalized_event"))).alias("normalized_event"),
                                    col("loyaltycurrency"),
                                    substring(col("pointdate"), 1, 10).alias("event_date"),
                                )

    family_mapping_df = (dynamic_expansion_df.select(
                                                    trim(col("cardnumber")).alias("cardnumber"),
                                                    trim(col("root_purchase_transnumber")).alias(
                                                        "root_purchase_transnumber"
                                                    ),
                                                    trim(col("transnumber")).alias("transnumber"),
                                                )
                                                .distinct()
                                            )

    family_event_df = (event_df.join(
                                    family_mapping_df,
                                    on=["cardnumber", "transnumber"],
                                    how="inner")
                    )
    return family_event_df