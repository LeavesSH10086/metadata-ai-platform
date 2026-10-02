from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max as spark_max, trim


def build_final_table_df(classified_summary_df: DataFrame,
                         family_event_df: DataFrame,
                        ) -> DataFrame:
    root_purchase_date_df = (family_event_df.filter(
                                                (col("transnumber") == col("root_purchase_transnumber"))
                                                & (col("normalized_event") == "PURCHASE")
                                            )
                                                .groupBy("banner", "cardnumber", "root_purchase_transnumber")
                                            .agg(spark_max("event_date").alias("root_purchase_date"))
                            )

        classified_family_df = (classified_summary_df.select(trim(col("banner")).alias("banner"),
                                                            trim(col("cardnumber")).alias("cardnumber"),
                                                        trim(col("root_purchase_transnumber")).alias(
                                                            "root_purchase_transnumber"
                                                        ),
                                                        col("scenario_name"),
                                                    ).distinct()
                        )

    final_df = (family_event_df.join(root_purchase_date_df,
                                    on=["banner", "cardnumber", "root_purchase_transnumber"],
                                    how="left",
                                )
                                .join(classified_family_df,
                                    on=["banner", "cardnumber", "root_purchase_transnumber"],
                                    how="inner",
                                )
                                .select(
                                        "scenario_name",
                                        "banner",
                                        "cardnumber",
                                        "root_purchase_date",
                                        "root_purchase_transnumber",
                                        "event_date",
                                        "transnumber"
                                )
                                .distinct()
                        )
    return final_df