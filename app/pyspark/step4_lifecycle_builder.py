from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    countDistinct,
    max as spark_max,
    substring,
    sum as spark_sum,
    trim,
    upper,
    when
)


def build_family_summary_df(
                            normalized_df: DataFrame,
                            dynamic_expansion_df: DataFrame,
                        ) -> DataFrame:
    # Do not hard-code scenario transaction numbers here.
    event_df = normalized_df.select(
                                    trim(col("cardnumber")).alias("cardnumber"),
                                    trim(col("transnumber")).alias("transnumber"),
                                    trim(col("original_transaction_num")).alias(
                                        "original_transaction_num"
                                    ),
                                    upper(trim(col("normalized_event"))).alias("normalized_event"),
                                    col("loyaltycurrency"),
                                    substring(col("pointdate"), 1, 10).alias("event_date")
                                )

    # One transaction can have several normalized rows. Record whether
    # each possible parent transaction contains RETURN or EXCHANGE.
    parent_type_df = (
                        event_df.groupBy("cardnumber", "transnumber")
                        .agg(
                            spark_max(
                                when(col("normalized_event") == "RETURN", 1).otherwise(0)
                            ).alias("parent_is_return"),
                            spark_max(
                                when(col("normalized_event") == "EXCHANGE", 1).otherwise(0)
                            ).alias("parent_is_exchange"),
                        )
                        .select(
                            col("cardnumber").alias("parent_cardnumber"),
                            col("transnumber").alias("parent_transnumber"),
                            "parent_is_return",
                            "parent_is_exchange",
                        )
                    )

    # Classify VOID by looking up the event represented by
    # original_transaction_num. This also supports self-references:
    # VOID transnumber=0076, original_transaction_num=0076, where another
    # row for 0076 is RETURN.
    classified_event_df = (
                            event_df.alias("event")
                            .join(
                                parent_type_df.alias("parent"),
                                (
                                    col("event.cardnumber")
                                    == col("parent.parent_cardnumber")
                                )
                                & (
                                    col("event.original_transaction_num")
                                    == col("parent.parent_transnumber")
                                ),
                                "left",
                            )
                            .select(
                                        col("event.cardnumber").alias("cardnumber"),
                                        col("event.transnumber").alias("transnumber"),
                                        col("event.loyaltycurrency").alias("loyaltycurrency"),
                                        when(
                                            (col("event.normalized_event") == "VOID")
                                            & (col("parent.parent_is_return") == 1),
                                            "VOID_RETURN",
                                        )
                                        .when(
                                            (col("event.normalized_event") == "VOID")
                                            & (col("parent.parent_is_exchange") == 1),
                                            "VOID_EXCHANGE",
                                        )
                                        .otherwise(col("event.normalized_event"))
                                        .alias("effective_event"),
                                    )
                        )

    family_mapping_df = (
                        dynamic_expansion_df.select(
                            trim(col("cardnumber")).alias("cardnumber"),
                            trim(col("root_purchase_transnumber")).alias(
                                "root_purchase_transnumber"
                            ),
                            trim(col("transnumber")).alias("transnumber"),
                        )
                        .distinct()
                    )

    family_df = classified_event_df.join(
                                        family_mapping_df,
                                        on=["cardnumber", "transnumber"],
                                        how="inner",
                                    )

    family_summary_df =  family_df.groupBy(
                                        "cardnumber",
                                        "root_purchase_transnumber",
                                    ).agg(
                                        countDistinct(
                                            when(
                                                (col("transnumber") == col("root_purchase_transnumber"))
                                                & (col("effective_event") == "PURCHASE"),
                                                col("transnumber"),
                                            )
                                        ).alias("root_purchase_count"),

                                        countDistinct(
                                            when(
                                                col("transnumber") != col("root_purchase_transnumber"),
                                                col("transnumber"),
                                            )
                                        ).alias("reposting_count"),

                                        countDistinct(
                                            when(
                                                col("effective_event") == "RETURN",
                                                col("transnumber"),
                                            )
                                        ).alias("return_count"),

                                        countDistinct(
                                            when(
                                                col("effective_event") == "EXCHANGE",
                                                col("transnumber"),
                                            )
                                        ).alias("exchange_count"),
                                        countDistinct(
                                            when(
                                                col("effective_event") == "VOID_PURCHASE",
                                                col("transnumber"),
                                            )
                                        ).alias("void_purchase_count"),

                                        countDistinct(
                                            when(
                                                col("effective_event") == "VOID_RETURN",
                                                col("transnumber"),
                                            )
                                        ).alias("void_return_count"),

                                        countDistinct(
                                            when(
                                                col("effective_event") == "VOID_EXCHANGE",
                                                col("transnumber"),
                                            )
                                        ).alias("void_exchange_count"),

                                        countDistinct(
                                            when(
                                                col("effective_event").isin("COMMIT_REDEEM", "REDEEM"),
                                                col("transnumber"),
                                            )
                                        ).alias("redeem_count"),

                                        spark_max(
                                                when(
                                                    (col("transnumber") == col("root_purchase_transnumber"))
                                                    & (col("effective_event") == "PURCHASE"),
                                                    col("event_date"),
                                                )
                                            ).alias("root_purchase_date"),

                                        spark_max(
                                                when(
                                                    col("effective_event") == "RETURN",
                                                    col("event_date"),
                                                )
                                            ).alias("last_return_date"),

                                        spark_sum("loyaltycurrency").alias("final_balance"),
                                    )
    family_summary_df = family_summary_df.withColumn(
                                            "day_context_same_flag",
                                            when(
                                                col("last_return_date").isNotNull()
                                                & (col("root_purchase_date") == col("last_return_date")),
                                                1,
                                            ).otherwise(0)
                                        )
    return family_summary_df