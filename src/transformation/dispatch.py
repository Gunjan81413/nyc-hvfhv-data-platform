from pyspark.sql.functions import col, lit


def update_dispatch_dimension(
    spark,
    source_month,
    silver_table,
    dispatch_table
):
    """
    Add any new dispatch bases found in the given month's Silver data.
    Existing dispatch keys are preserved.
    """

    print(f"Updating dispatch dimension for: {source_month}")

    # Read only the current month
    df_silver = (
        spark.table(silver_table)
        .filter(col("_source_month") == source_month)
    )

    # Get dispatching + originating bases
    dispatching = (
        df_silver
        .select(col("dispatching_base_num").alias("base_num"))
        .filter(col("base_num").isNotNull())
    )

    originating = (
        df_silver
        .select(col("originating_base_num").alias("base_num"))
        .filter(col("base_num").isNotNull())
    )

    new_bases = (
        dispatching
        .union(originating)
        .distinct()
    )

    # Existing dimension
    if spark.catalog.tableExists(dispatch_table):

        existing = (
            spark.table(dispatch_table)
            .select("base_num")
            .distinct()
        )

        # Only bases that don't already exist
        new_bases = new_bases.join(
            existing,
            on="base_num",
            how="left_anti"
        )

        new_count = new_bases.count()

        print(f"New dispatch bases found: {new_count}")

        if new_count == 0:
            print("No new dispatch bases. Dimension is already up to date.")
            return spark.table(dispatch_table)

        # Find current maximum key
        max_key = (
            spark.table(dispatch_table)
            .agg({"dispatch_key": "max"})
            .first()[0]
        )

        if max_key is None:
            max_key = 0

        # Assign new keys
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number

        window = Window.orderBy("base_num")

        new_bases = (
            new_bases
            .withColumn(
                "dispatch_key",
                col("base_num")
            )
            .select(
                "dispatch_key",
                "base_num"
            )
        )

        # Append new bases
        (
            new_bases
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(dispatch_table)
        )

        print("Dispatch dimension updated successfully.")

    else:
        # First-time creation
        from pyspark.sql.functions import row_number
        from pyspark.sql.window import Window

        window = Window.orderBy("base_num")

        df_dispatch = (
            new_bases
            .withColumn(
                "dispatch_key",
                row_number().over(window)
            )
            .select("dispatch_key", "base_num")
        )

        (
            df_dispatch
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(dispatch_table)
        )

        print("Dispatch dimension created successfully.")

    return spark.table(dispatch_table)