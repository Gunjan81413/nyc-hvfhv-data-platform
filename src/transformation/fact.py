from pyspark.sql.functions import col, to_date, date_format, xxhash64


def build_month_to_fact(
    spark,
    source_month,
    silver_table,
    date_table,
    provider_table,
    location_table,
    dispatch_table,
    fact_table
):
    """
    Build one month's Silver data into the Fact Trip table.
    """

    print(f"Processing Fact for: {source_month}")

    # --------------------------------------------------
    # 1. Idempotency check
    # --------------------------------------------------

    if spark.catalog.tableExists(fact_table):

        existing_rows = (
            spark.table(fact_table)
            .filter(col("_source_month") == source_month)
            .limit(1)
            .count()
        )

        if existing_rows > 0:
            print(f"{source_month} already exists in Fact.")
            print("Skipping to prevent duplicates.")
            return None

    # --------------------------------------------------
    # 2. Read only requested month from Silver
    # --------------------------------------------------

    df_silver = (
        spark.table(silver_table)
        .filter(col("_source_month") == source_month)
    )

    print(
        f"Silver rows for {source_month}: "
        f"{df_silver.count():,}"
    )

    # --------------------------------------------------
    # 3. Create technical trip key
    # --------------------------------------------------

    df_fact = (
        df_silver
        .withColumn(
            "trip_key",
            xxhash64(
                "hvfhs_license_num",
                "request_datetime",
                "pickup_datetime",
                "dropoff_datetime",
                "PULocationID",
                "DOLocationID",
                "trip_miles",
                "trip_time",
                "base_passenger_fare",
                "driver_pay"
            )
        )
    )

    # --------------------------------------------------
    # 4. Date key
    # --------------------------------------------------

    df_fact = (
        df_fact
        .withColumn(
            "date_key",
            date_format(
                to_date("pickup_datetime"),
                "yyyyMMdd"
            ).cast("int")
        )
    )

    # --------------------------------------------------
    # 5. Provider key
    # --------------------------------------------------

    provider_dim = (
        spark.table(provider_table)
        .select(
            col("provider_code"),
            col("provider_key")
        )
    )

    df_fact = (
        df_fact
        .join(
            provider_dim,
            df_fact.hvfhs_license_num ==
            provider_dim.provider_code,
            "left"
        )
        .drop("provider_code")
    )

    # --------------------------------------------------
    # 6. Location keys
    # --------------------------------------------------

    location_dim = (
        spark.table(location_table)
        .select(
            col("location_id"),
            col("location_key")
        )
    )

    pickup_dim = location_dim.select(
        col("location_id").alias("pickup_location_id"),
        col("location_key").alias("pickup_location_key")
    )

    dropoff_dim = location_dim.select(
        col("location_id").alias("dropoff_location_id"),
        col("location_key").alias("dropoff_location_key")
    )

    df_fact = (
        df_fact
        .join(
            pickup_dim,
            df_fact.PULocationID ==
            pickup_dim.pickup_location_id,
            "left"
        )
        .join(
            dropoff_dim,
            df_fact.DOLocationID ==
            dropoff_dim.dropoff_location_id,
            "left"
        )
        .drop(
            "pickup_location_id",
            "dropoff_location_id"
        )
    )

    # --------------------------------------------------
    # 7. Dispatch keys
    # --------------------------------------------------

    dispatch_dim = (
        spark.table(dispatch_table)
        .select(
            col("base_num"),
            col("dispatch_key")
        )
    )

    dispatching_dim = dispatch_dim.select(
        col("base_num").alias("dispatching_base_num_dim"),
        col("dispatch_key").alias("dispatching_base_key")
    )

    originating_dim = dispatch_dim.select(
        col("base_num").alias("originating_base_num_dim"),
        col("dispatch_key").alias("originating_base_key")
    )

    df_fact = (
        df_fact
        .join(
            dispatching_dim,
            df_fact.dispatching_base_num ==
            dispatching_dim.dispatching_base_num_dim,
            "left"
        )
        .join(
            originating_dim,
            df_fact.originating_base_num ==
            originating_dim.originating_base_num_dim,
            "left"
        )
        .drop(
            "dispatching_base_num_dim",
            "originating_base_num_dim"
        )
    )

    # --------------------------------------------------
    # 8. Select Fact columns
    # --------------------------------------------------

    fact_columns = [
        "trip_key",
        "date_key",
        "provider_key",
        "pickup_location_key",
        "dropoff_location_key",
        "dispatching_base_key",
        "originating_base_key",

        "request_datetime",
        "on_scene_datetime",
        "pickup_datetime",
        "dropoff_datetime",

        "trip_miles",
        "trip_time",
        "calculated_trip_time_seconds",

        "base_passenger_fare",
        "tolls",
        "bcf",
        "sales_tax",
        "congestion_surcharge",
        "airport_fee",
        "tips",
        "driver_pay",
        "cbd_congestion_fee",

        "shared_request",
        "shared_match",
        "access_a_ride",
        "wav_request",
        "wav_match",

        "customer_wait_seconds",
        "driver_response_seconds",

        "trip_time_consistent",
        "timestamp_quality_flag",
        "financial_quality_flag",
        "overall_quality_status",

        "_source_month",
        "_source_file",
        "_ingested_at"
    ]

    df_fact = df_fact.select(fact_columns)

    # --------------------------------------------------
    # 9. Write Fact
    # --------------------------------------------------

    if spark.catalog.tableExists(fact_table):

        (
            df_fact
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(fact_table)
        )

    else:

        (
            df_fact
            .write
            .format("delta")
            .mode("overwrite")
            .partitionBy("_source_month")
            .saveAsTable(fact_table)
        )

    print(
        f"{source_month} successfully written to Fact."
    )

    return df_fact