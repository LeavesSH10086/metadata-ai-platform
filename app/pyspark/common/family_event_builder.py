from pyspark.sql import DataFrame
from pyspark.sql.functions import col, countDistinct, substring, trim, upper, when


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

    mapping_count_df = family_mapping_df.groupBy("cardnumber", "transnumber").agg(
        countDistinct("root_purchase_transnumber").alias("family_mapping_count")
    )

    direct_root_df = family_mapping_df.filter(
        col("root_purchase_transnumber") == col("transnumber")
    ).select(
        "cardnumber",
        col("root_purchase_transnumber").alias("direct_root_transnumber"),
    ).distinct()

    event_df = (event_df.withColumn(
                            "owner_transnumber",
                            when(
                                col("normalized_event") == "REPOSTING",
                                col("transnumber"),
                            )
                            .when(
                                col("original_transaction_num").isNotNull()
                                & (col("original_transaction_num") != ""),
                                col("original_transaction_num"),
                            )
                            .otherwise(col("transnumber")),
                        )
                        .alias("event")
                        .join(
                            direct_root_df.alias("root"),
                            (col("event.cardnumber") == col("root.cardnumber"))
                            & (
                                col("event.owner_transnumber")
                                == col("root.direct_root_transnumber")
                            ),
                            how="left",
                        )
                        .select(
                            col("event.*"),
                            col("root.direct_root_transnumber"),
                        )
                )

    family_event_df = (event_df.join(
                                    family_mapping_df,
                                    on=["cardnumber", "transnumber"],
                                    how="inner")
                               .join(
                                   mapping_count_df,
                                   on=["cardnumber", "transnumber"],
                                   how="inner",
                               )
                               .filter(
                                   (col("family_mapping_count") == 1)
                                   | col("direct_root_transnumber").isNull()
                                   | (
                                       col("root_purchase_transnumber")
                                       == col("direct_root_transnumber")
                                   )
                               )
                               .drop(
                                   "owner_transnumber",
                                   "direct_root_transnumber",
                                   "family_mapping_count",
                               )
                    )
    return family_event_df