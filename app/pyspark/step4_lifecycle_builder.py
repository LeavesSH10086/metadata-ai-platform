from pyspark.sql import DataFrame
from pyspark.sql.functions import col, countDistinct, max as spark_max, sum as spark_sum, when


def build_family_summary_df(family_event_df: DataFrame) -> DataFrame:
    """Aggregate transaction-family events into lifecycle classification metrics.

    The builder resolves VOID event types from their referenced transactions,
    calculates event counts and balances for each purchase family, and adds
    exchange price comparison fields when the family contains an exchange.

    Args:
        family_event_df: Event-level family data containing banner, card,
            transaction, event type, date, and loyalty currency fields.

    Returns:
        One row per banner, card, and root purchase with lifecycle counts,
        balance metrics, root purchase date, and exchange price details.
    """

    # One transaction can have several normalized rows. Record whether
    # each possible parent transaction contains RETURN or EXCHANGE.
    parent_type_df = (
                        family_event_df.groupBy("cardnumber", "transnumber", "banner")
                        .agg(spark_max(when(col("normalized_event") == "RETURN", 1).otherwise(0)).alias("parent_is_return"),
                            spark_max(when(col("normalized_event") == "EXCHANGE", 1).otherwise(0)).alias("parent_is_exchange"),
                            spark_max(when((col("normalized_event") == "VOID")&(col("transnumber") == col("original_transaction_num")), 1).otherwise(0)).alias("parent_is_purchase")
                            )
                        .select(col("cardnumber").alias("parent_cardnumber"),
                                col("transnumber").alias("parent_transnumber"),
                                col("banner").alias("parent_banner"),
                                "parent_is_return",
                                "parent_is_exchange",
                                "parent_is_purchase"
                            )
                    )

    # Classify VOID by looking up the event represented by
    # original_transaction_num. This also supports self-references:
    # VOID transnumber=0076, original_transaction_num=0076, where another
    # row for 0076 is RETURN.
    classified_event_df = (family_event_df.alias("event").join(parent_type_df.alias("parent"),
                                                        (col("event.cardnumber") == col("parent.parent_cardnumber"))
                                                        & (col("event.original_transaction_num") == col("parent.parent_transnumber"))
                                                        & (col("event.banner") == col("parent.parent_banner")),
                                                        "left"
                                                    )
                                                .select(col("event.banner").alias("banner"),
                                                        col("event.cardnumber").alias("cardnumber"),
                                                        col("event.root_purchase_transnumber").alias("root_purchase_transnumber"),
                                                        col("event.transnumber").alias("transnumber"),
                                                        col("event.loyaltycurrency").alias("loyaltycurrency"),
                                                        col("event.event_date").alias("event_date"),
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
                                                        .when(
                                                            (col("event.normalized_event") == "VOID")
                                                            & (col("parent.parent_is_purchase") == 1),
                                                            "VOID_PURCHASE",
                                                        )
                                                        .otherwise(col("event.normalized_event"))
                                                        .alias("effective_event"),
                                                    )
                        )

    # Use the classified event rows as the event-level source for all family
    # aggregates below.
    family_df = classified_event_df

    # Capture the purchase date for each root transaction so later events can
    # be compared with the family's originating purchase date.
    root_purchase_date_df = (family_df.filter((col("transnumber") == col("root_purchase_transnumber"))
                                                 & (col("effective_event") == "PURCHASE")
                                                 )
                                    .groupBy("banner", "cardnumber", "root_purchase_transnumber")
                                    .agg(spark_max("event_date").alias("root_purchase_date"))
                            )

        # Attach the root purchase date to every classified event in its family.
    family_with_root_date_df = family_df.join(root_purchase_date_df,
                                              on=["banner", "cardnumber", "root_purchase_transnumber"],
                                              how="left",
                                            )

        # Produce one summary row per family with distinct event counts, same-day
        # context, and the resulting loyalty currency balance.
    family_summary_df =  family_with_root_date_df.groupBy("banner", "cardnumber", 
                                                          "root_purchase_transnumber",
                                                          "root_purchase_date"
                                                    ).agg(
                                                        countDistinct(
                                                                        when(
                                                                            (col("transnumber") == col("root_purchase_transnumber"))
                                                                            & (col("effective_event") == "PURCHASE"),
                                                                            col("transnumber"),
                                                                        )
                                                                    ).alias("root_purchase_count"),

                                                        countDistinct(
                                                                        when((col("effective_event") == "REPOSTING"),
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
                                                                        col("effective_event").isin("RETURN", "EXCHANGE")
                                                                        & col("root_purchase_date").isNotNull()
                                                                        & (col("event_date") == col("root_purchase_date")),
                                                                        1,
                                                                    ).otherwise(0)
                                                                ).alias("day_context_same_flag"),

                                                        spark_sum("loyaltycurrency").alias("final_balance"),
                                    )

    # Identify family keys that need exchange-specific price calculations.
    exchange_families_df = (family_summary_df.filter(col("exchange_count") > 0)
                                             .select("banner","cardnumber", "root_purchase_transnumber").distinct())


    # Restrict the event-level input to transactions belonging to exchange
    # families and retain only fields needed for price comparison.
    exchange_family_events_df = (exchange_families_df.alias("summary").join(family_event_df.alias("event"),
                                                        (col("summary.cardnumber") == col("event.cardnumber"))
                                                        & (col("summary.root_purchase_transnumber") == col("event.root_purchase_transnumber")),
                                                        how="inner")
                                                    .select(col("summary.banner").alias("banner"),
                                                            col("summary.cardnumber").alias("cardnumber"),
                                                            col("summary.root_purchase_transnumber").alias("root_purchase_transnumber"),
                                                            col("event.transnumber").alias("transnumber"),
                                                            col("event.normalized_event").alias("normalized_event"),
                                                            col("event.loyaltycurrency").alias("loyaltycurrency"))
                            )

    # Compare reposted loyalty currency with the original purchase amount and
    # label the exchange as LOWER, HIGHER, or EQUAL in value.
    exchange_price_comparison_df = (exchange_family_events_df.groupBy("banner", "cardnumber", "root_purchase_transnumber")
                                                         .agg(spark_sum(when(
                                                                            (col("transnumber") == col("root_purchase_transnumber"))
                                                                            & (col("normalized_event") == "PURCHASE"),
                                                                            col("loyaltycurrency")
                                                                        ).otherwise(0)

                                                                    ).alias("original_purchase_loyalty_currency"),
                                                            spark_sum(when(
                                                                            col("normalized_event") == "REPOSTING", col("loyaltycurrency")
                                                                            ).otherwise(0)
                                                                    ).alias("reposting_loyalty_currency")
                                                                )
                                                            .withColumn("exchange_price_difference", col("reposting_loyalty_currency") - col("original_purchase_loyalty_currency"))
                                                            .withColumn("exchange_price_comparison", when(col("exchange_price_difference") < 0, "LOWER")
                                                                                                    .when(col("exchange_price_difference") > 0, "HIGHER")
                                                                                                    .otherwise("EQUAL")
                                                                    )
                                                            )

    # Add optional exchange price metrics to the common lifecycle summary;
    # non-exchange families retain null exchange-specific columns.
    family_summary_final_df = (family_summary_df.join(exchange_price_comparison_df,
                                                     on=["banner", "cardnumber", "root_purchase_transnumber"],
                                                     how="left"
                                                     ))
    # family_summary_final_df.show(50, truncate=False)

        
    return family_summary_final_df