from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, sum

def build_family_summary_df(dynamic_expansion_df: DataFrame, 
                            normalized_df: DataFrame):
    family_df = dynamic_expansion_df.join(normalized_df, on="transnumber", how="inner")
    summary_df = (family_df.groupBy("root_purchase_transnumber")
                            .agg(sum(when(col("normalized_event") == "PURCHASE", 1).otherwise(0)).alias("purchase_count"),
                                 sum(when(col("normalized_event") == "RETURN", 1).otherwise(0)).alias("return_count"),
                                 sum(when(col("normalized_event") == "EXCHANGE", 1).otherwise(0)).alias("exchange_count"),
                                 sum(when(col("normalized_event") == "VOID_RETURN", 1).otherwise(0)).alias("cancel_count"),
                                 sum(when(col("normalized_event").isin("COMMIT_REDEEM", "REDEEM"), 1).otherwise(0)).alias("redeem_count"),
                                 sum(col("loyaltycurrency")).alias("final_balance")
                            )
)
    return summary_df