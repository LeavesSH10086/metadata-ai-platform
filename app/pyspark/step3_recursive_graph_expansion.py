from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from app.util.logger import LogMixin

logger = LogMixin().logger

def build_recursive_graph_expansion_df(normalized_df: DataFrame) -> DataFrame:
    """
    This function performs recursive graph expansion on the normalized dataframe.
    It iteratively expands the graph based on the defined relationships until no further expansion is possible.
    """

    root_purchase_df = (normalized_df.filter((col("normalized_event")=="PURCHASE")
                                              & (col("transnumber")==col("originating_transnumber"))
                                              )
                                    .select(col("transnumber").alias("root_purchase_transnumber"), 
                                            "cardnumber")
                        ).distinct()
    known_df = root_purchase_df.select("cardnumber", 
                                       "root_purchase_transnumber", 
                                       col("root_purchase_transnumber").alias("transnumber")
                                       ).distinct()
    previous_count = -1
    current_count = known_df.count()
    # dynamic_expansion_df = known_df
    if current_count != previous_count:
        expanded_df = (known_df.alias("known").join(normalized_df.alias("normalized"), 
                                                             (col("known.transnumber") == col("normalized.original_transaction_num")) 
                                                             |
                                                             (col("known.cardnumber") == col("normalized.originating_cardnumber"))
                                                             | 
                                                             (col("known.transnumber") == col("normalized.transnumber")), 
                                                             "inner")
                                                             .select(col("normalized.cardnumber"),
                                                                     col("known.root_purchase_transnumber"),
                                                                     col("normalized.transnumber")
                                                                     ).distinct()
                                    )
        dynamic_expansion_df = known_df.unionByName(expanded_df).distinct()
    else:
        dynamic_expansion_df = known_df
        current_count = known_df.count()
        logger.info(f"Previous count: {previous_count}, Current count: {current_count}")
    
    return dynamic_expansion_df
    
   