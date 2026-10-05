from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, trim


def build_final_table_df(classified_summary_df: DataFrame,
                         family_event_df: DataFrame,
                        ) -> DataFrame:
    classified_family_df = (classified_summary_df.select(trim(col("banner")).alias("banner"),
                                                        trim(col("cardnumber")).alias("cardnumber"),
                                                        trim(col("root_purchase_transnumber")).alias(
                                                            "root_purchase_transnumber"
                                                        ),
                                                        col("root_purchase_date"),
                                                        col("scenario_name"),
                                                    ).distinct()
                        )

    final_df = (family_event_df.join(classified_family_df,
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
                                        "transnumber",
                                        current_timestamp().alias("pipelne_operation_ts"),
                                )
                                .distinct()
                        )
    return final_df