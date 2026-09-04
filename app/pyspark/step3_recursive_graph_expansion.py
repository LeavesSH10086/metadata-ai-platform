from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper

from app.util.logger import LogMixin

logger = LogMixin().logger


def build_recursive_graph_expansion_df(
    normalized_df,
    max_iterations: int = 100,
) :
    """
    Builds transaction families using breadth-first graph expansion.

    Each output row contains:
      - cardnumber
      - root_purchase_transnumber
      - transnumber

    Only newly discovered transactions are expanded in each iteration.
    """

    root_purchase_df = (normalized_df.filter(
                                            (upper(trim(col("normalized_event"))) == "PURCHASE")
                                            & (col("transnumber") == col("originating_transnumber"))
                                        )
                                        .select(
                                            "cardnumber",
                                            col("transnumber").alias("root_purchase_transnumber"),
                                            col("transnumber"),
                                        )
                                        .filter(
                                            col("cardnumber").isNotNull()
                                            & col("transnumber").isNotNull()
                                        )
                                        .distinct()
                                    )

    direct_members_df = (normalized_df.filter(col('transnumber') != col('originating_transnumber')).alias("event")
                                    .join(root_purchase_df.alias("root"),
                                            (col("event.cardnumber") == col("root.cardnumber"))
                                            & (
                                                col("event.originating_transnumber") == col("root.root_purchase_transnumber")
                                            ),
                                            "inner",
                                        )
                                    .select(
                                        col("event.cardnumber"),
                                        col("root.root_purchase_transnumber"),
                                        col("event.transnumber"),
                                    )
                                    .distinct()
                                    )
    print("Normalized transactions:", normalized_df.select( "cardnumber", "transnumber").distinct().count())
    print("Transactions mapped directly to a root:", direct_members_df.count())
    
    # Convert the OR relationship into a regular parent-child edge table.
    original_edges_df = normalized_df.select(
        "cardnumber",
        col("original_transaction_num").alias("parent_transnumber"),
        col("transnumber").alias("child_transnumber"),
    )

    originating_edges_df = normalized_df.select(
        "cardnumber",
        col("originating_transnumber").alias("parent_transnumber"),
        col("transnumber").alias("child_transnumber"),
    )

    edges_df = (
        original_edges_df.unionByName(originating_edges_df)
        .filter(
            col("cardnumber").isNotNull()
            & col("parent_transnumber").isNotNull()
            & col("child_transnumber").isNotNull()
        )
        .distinct()
    )

    # visited contains all discovered transactions.
    # frontier contains only transactions discovered in the last iteration.
    visited_df = root_purchase_df
    frontier_df = root_purchase_df

    visited_count = visited_df.count()
    print(f"Recursive graph expansion root count: {visited_count}")

    for iteration in range(1, max_iterations + 1):
        candidate_df = (
            frontier_df.alias("frontier")
            .join(
                edges_df.alias("edge"),
                (col("frontier.cardnumber") == col("edge.cardnumber"))
                & (
                    col("frontier.transnumber")
                    == col("edge.parent_transnumber")
                ),
                "inner",
            )
            .select(
                col("edge.cardnumber").alias("cardnumber"),
                col("frontier.root_purchase_transnumber"),
                col("edge.child_transnumber").alias("transnumber"),
            )
            .distinct()
        )

        # Exclude transactions already discovered for the same root family.
        next_frontier_df = (
            candidate_df.join(
                visited_df,
                on=[
                    "cardnumber",
                    "root_purchase_transnumber",
                    "transnumber",
                ],
                how="left_anti",
            )
        )

        new_count = next_frontier_df.count()

        print(
            f"Recursive graph expansion iteration {iteration}: "
            f"visited={visited_count}, newly discovered={new_count}"
        )

        if new_count == 0:
            next_frontier_df.unpersist()
            frontier_df.unpersist()
            edges_df.unpersist()
            return visited_df

        updated_visited_df = (
            visited_df.unionByName(next_frontier_df)
        )
        updated_count = updated_visited_df.count()

        old_visited_df = visited_df
        old_frontier_df = frontier_df

        visited_df = updated_visited_df
        frontier_df = next_frontier_df
        visited_count = updated_count

        old_frontier_df.unpersist()
        if old_visited_df is not old_frontier_df:
            old_visited_df.unpersist()

    edges_df.unpersist()
    frontier_df.unpersist()
    visited_df.unpersist()

    raise RuntimeError(
        "Recursive graph expansion exceeded "
        f"{max_iterations} iterations. Check transaction relationships "
        "for unexpectedly long or highly connected families."
    )