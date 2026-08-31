from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper

from app.util.logger import LogMixin

logger = LogMixin().logger


def build_recursive_graph_expansion_df(
    normalized_df: DataFrame,
    max_iterations: int = 100,
) -> DataFrame:
    """
    Builds transaction families using breadth-first graph expansion.

    Each output row contains:
      - cardnumber
      - root_purchase_transnumber
      - transnumber

    Only newly discovered transactions are expanded in each iteration.
    """

    root_purchase_df = (
        normalized_df.filter(
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
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

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
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # visited contains all discovered transactions.
    # frontier contains only transactions discovered in the last iteration.
    visited_df = root_purchase_df
    frontier_df = root_purchase_df

    visited_count = visited_df.count()
    logger.info("Recursive graph expansion root count: %s", visited_count)

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
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        new_count = next_frontier_df.count()

        logger.info(
            "Recursive graph expansion iteration %s: "
            "visited=%s, newly discovered=%s",
            iteration,
            visited_count,
            new_count,
        )

        if new_count == 0:
            next_frontier_df.unpersist()
            frontier_df.unpersist()
            edges_df.unpersist()
            return visited_df

        updated_visited_df = (
            visited_df.unionByName(next_frontier_df)
            .persist(StorageLevel.MEMORY_AND_DISK)
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

    raise RuntimeError(
        "Recursive graph expansion exceeded "
        f"{max_iterations} iterations. Check transaction relationships "
        "for unexpectedly long or highly connected families."
    )