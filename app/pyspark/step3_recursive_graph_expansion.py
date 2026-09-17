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
    Build transaction families using breadth-first graph expansion.

    A root purchase starts a family. The function follows the normalized
    ``predecessor_transnumber`` link from each discovered transaction until
    no new family members remain.

    Args:
        normalized_df: Normalized transaction events and relationship fields.
        max_iterations: Maximum graph depth before treating the data as
            unexpectedly connected.

    Returns:
        A DataFrame with ``cardnumber``, ``root_purchase_transnumber``, and
        ``transnumber`` for every discovered member of each purchase family.

    Raises:
        RuntimeError: If expansion does not converge within ``max_iterations``.
    """

    # Identify each original purchase that can serve as a family root.
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

    # Step 2 resolves the two raw relationship fields into one canonical
    # predecessor. Using it avoids doubling the edge table and avoids paths
    # that disagree with the normalization precedence rule.
    root_transaction_df = root_purchase_df.select(
        "cardnumber",
        col("transnumber").alias("child_transnumber"),
    )

    edges_df = (
        normalized_df.select(
            trim(col("cardnumber")).alias("cardnumber"),
            trim(col("predecessor_transnumber")).alias("parent_transnumber"),
            trim(col("transnumber")).alias("child_transnumber"),
        )
        .filter(
            col("cardnumber").isNotNull()
            & col("parent_transnumber").isNotNull()
            & col("child_transnumber").isNotNull()
            & (col("parent_transnumber") != col("child_transnumber"))
        )
        .distinct()
        .join(
            root_transaction_df,
            on=["cardnumber", "child_transnumber"],
            how="left_anti",
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # Seed traversal state. visited_df contains every discovered member;
    # frontier_df contains only members whose outgoing edges remain to explore.
    visited_df = root_purchase_df.persist(StorageLevel.MEMORY_AND_DISK)
    frontier_df = visited_df

    visited_count = visited_df.count()
    print(f"Recursive graph expansion root count: {visited_count}")

    for iteration in range(1, max_iterations + 1):
        # Expand one graph level from the current frontier while preserving the
        # root purchase assigned to each path.
        candidate_df = (frontier_df.alias("frontier").join(
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

        # Keep only members not already discovered for this card and root. The
        # anti-join also prevents cycles from expanding forever.
        next_frontier_df = (candidate_df.join(
                                                visited_df,
                                                on=[
                                                    "cardnumber",
                                                    "root_purchase_transnumber",
                                                    "transnumber",
                                                ],
                                                how="left_anti",
                                            )
                        ).persist(StorageLevel.MEMORY_AND_DISK)

        new_count = next_frontier_df.count()

        print(
            f"Recursive graph expansion iteration {iteration}: "
            f"visited={visited_count}, newly discovered={new_count}"
        )

        if new_count == 0:
            # An empty frontier means the graph has converged. visited_df is
            # now the complete transaction-to-family mapping.
            next_frontier_df.unpersist()
            frontier_df.unpersist()
            edges_df.unpersist()
            return visited_df

        # Add the newly discovered graph level and make it the next frontier.
        updated_visited_df = visited_df.unionByName(next_frontier_df).persist(
            StorageLevel.MEMORY_AND_DISK
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

    # Reaching this point means traversal did not converge within the guard
    # limit, so release cached plans before reporting the data issue.
    edges_df.unpersist()
    frontier_df.unpersist()
    visited_df.unpersist()

    raise RuntimeError(
        "Recursive graph expansion exceeded "
        f"{max_iterations} iterations. Check transaction relationships "
        "for unexpectedly long or highly connected families."
    )