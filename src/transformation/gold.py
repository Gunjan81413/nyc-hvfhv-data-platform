from pyspark.sql.functions import (
    col,
    sum,
    avg,
    count,
    when,
    round
)


def build_daily_provider_gold(
    spark,
    source_month,
    fact_table,
    provider_table,
    date_table,
    gold_table
):
    """
    Build daily provider metrics for one new month.
    """

    print(f"Building Gold for: {source_month}")

    # ---------------------------------------------
    # 1. Idempotency
    # ---------------------------------------------

    if spark.catalog.tableExists(gold_table):

        existing = (
            spark.table(gold_table)
            .filter(
                col("date").cast("string").startswith(
                    source_month
                )
            )
            .limit(1)
            .count()
        )

        if existing > 0:
            print(
                f"{source_month} already exists in Gold."
            )
            print("Skipping.")
            return

    # ---------------------------------------------
    # 2. Read only new month from Fact
    # ---------------------------------------------

    df_fact = (
        spark.table(fact_table)
        .filter(col("_source_month") == source_month)
    )

    # ---------------------------------------------
    # 3. Join dimensions
    # ---------------------------------------------

    provider_dim = (
        spark.table(provider_table)
        .select(
            col("provider_key").alias("dim_provider_key"),
            col("provider_name")
        )
    )

    date_dim = (
        spark.table(date_table)
        .select(
            col("date_key").alias("dim_date_key"),
            col("date")
        )
    )

    df = (
        df_fact
        .join(
            provider_dim,
            df_fact.provider_key ==
            provider_dim.dim_provider_key,
            "left"
        )
        .join(
            date_dim,
            df_fact.date_key ==
            date_dim.dim_date_key,
            "left"
        )
        .drop(
            "dim_provider_key",
            "dim_date_key"
        )
    )

    # ---------------------------------------------
    # 4. Aggregate
    # ---------------------------------------------

    df_gold = (
        df
        .groupBy(
            "date_key",
            "date",
            "provider_key",
            "provider_name"
        )
        .agg(
            count("*").alias("total_trips"),

            sum("trip_miles")
            .alias("total_trip_miles"),

            avg("trip_miles")
            .alias("avg_trip_distance"),

            (
                avg("trip_time") / 60
            ).alias("avg_trip_duration_minutes"),

            (
                avg("customer_wait_seconds") / 60
            ).alias("avg_customer_wait_minutes"),

            sum("base_passenger_fare")
            .alias("total_passenger_fare"),

            sum("tips")
            .alias("total_tips"),

            sum("driver_pay")
            .alias("total_driver_pay"),

            sum(
                when(
                    col("overall_quality_status") != "GOOD",
                    1
                ).otherwise(0)
            ).alias("quality_issue_trips")
        )
    )

    # ---------------------------------------------
    # 5. Derived financial KPIs
    # ---------------------------------------------

    df_gold = (
        df_gold
        .withColumn(
            "fare_per_mile",
            when(
                col("total_trip_miles") > 0,
                col("total_passenger_fare")
                / col("total_trip_miles")
            )
        )
        .withColumn(
            "driver_pay_per_mile",
            when(
                col("total_trip_miles") > 0,
                col("total_driver_pay")
                / col("total_trip_miles")
            )
        )
    )

    # ---------------------------------------------
    # 6. Write
    # ---------------------------------------------

    (
        df_gold
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(gold_table)
    )

    print(
        f"{source_month} Gold successfully written."
    )

    return df_gold