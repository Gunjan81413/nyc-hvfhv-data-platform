from pyspark.sql.functions import (
    col,
    when,
    lit,
    unix_timestamp,
    abs
)


def transform_month_to_silver(
    spark,
    source_month,
    bronze_table,
    silver_table
):
    """
    Transform one month's Bronze data into Silver.
    """

    print(f"Processing month: {source_month}")

    # --------------------------------------------------
    # 1. Read Bronze
    # --------------------------------------------------

    df_bronze = spark.table(bronze_table)

    # --------------------------------------------------
    # 2. Idempotency check
    # --------------------------------------------------

    if spark.catalog.tableExists(silver_table):

        existing_rows = (
            spark.table(silver_table)
            .filter(col("_source_month") == source_month)
            .limit(1)
            .count()
        )

        if existing_rows > 0:
            print(
                f"{source_month} already exists in Silver."
            )
            print("Skipping to prevent duplicates.")
            return None

    # --------------------------------------------------
    # 3. Select only requested month
    # --------------------------------------------------

    df_month = (
        df_bronze
        .filter(col("_source_month") == source_month)
    )

    row_count = df_month.count()

    print(
        f"Bronze rows for {source_month}: "
        f"{row_count:,}"
    )

    # --------------------------------------------------
    # 4. Timestamp quality
    # --------------------------------------------------

    df_quality = (
        df_month
        .withColumn(
            "timestamp_quality_flag",
            when(
                (col("request_datetime") > col("on_scene_datetime")) |
                (col("on_scene_datetime") > col("pickup_datetime")) |
                (col("pickup_datetime") > col("dropoff_datetime")),
                lit("ISSUE")
            ).otherwise(lit("OK"))
        )
    )

    # --------------------------------------------------
    # 5. Financial quality
    # --------------------------------------------------

    df_financial = (
        df_quality
        .withColumn(
            "financial_quality_flag",
            when(
                (col("base_passenger_fare") < 0) |
                (col("driver_pay") < 0),
                lit("ISSUE")
            ).otherwise(lit("OK"))
        )
    )

    # --------------------------------------------------
    # 6. Derived time metrics
    # --------------------------------------------------

    df_derived = (
        df_financial

        .withColumn(
            "customer_wait_seconds",
            when(
                col("pickup_datetime") >= col("request_datetime"),
                unix_timestamp("pickup_datetime")
                - unix_timestamp("request_datetime")
            )
        )

        .withColumn(
            "driver_response_seconds",
            when(
                col("on_scene_datetime") >= col("request_datetime"),
                unix_timestamp("on_scene_datetime")
                - unix_timestamp("request_datetime")
            )
        )

        .withColumn(
            "calculated_trip_time_seconds",
            when(
                col("dropoff_datetime") >= col("pickup_datetime"),
                unix_timestamp("dropoff_datetime")
                - unix_timestamp("pickup_datetime")
            )
        )

        .withColumn(
            "trip_time_consistent",
            when(
                abs(
                    col("trip_time")
                    - col("calculated_trip_time_seconds")
                ) <= 3,
                True
            ).otherwise(False)
        )
    )

    # --------------------------------------------------
    # 7. Convert Y/N flags to Boolean
    # --------------------------------------------------

    df_flags = (
        df_derived

        .withColumn(
            "shared_request",
            when(col("shared_request_flag") == "Y", True)
            .when(col("shared_request_flag") == "N", False)
        )

        .withColumn(
            "shared_match",
            when(col("shared_match_flag") == "Y", True)
            .when(col("shared_match_flag") == "N", False)
        )

        .withColumn(
            "access_a_ride",
            when(col("access_a_ride_flag") == "Y", True)
            .when(col("access_a_ride_flag") == "N", False)
        )

        .withColumn(
            "wav_request",
            when(col("wav_request_flag") == "Y", True)
            .when(col("wav_request_flag") == "N", False)
        )

        .withColumn(
            "wav_match",
            when(col("wav_match_flag") == "Y", True)
            .when(col("wav_match_flag") == "N", False)
        )
    )

    # --------------------------------------------------
    # 8. Overall quality status
    # --------------------------------------------------

    df_final = (
        df_flags
        .withColumn(
            "overall_quality_status",
            when(
                (col("timestamp_quality_flag") == "ISSUE") &
                (col("financial_quality_flag") == "ISSUE"),
                "TIMESTAMP_AND_FINANCIAL_ISSUE"
            )
            .when(
                col("timestamp_quality_flag") == "ISSUE",
                "TIMESTAMP_ISSUE"
            )
            .when(
                col("financial_quality_flag") == "ISSUE",
                "FINANCIAL_ISSUE"
            )
            .when(
                col("trip_time_consistent") == False,
                "TRIP_TIME_ISSUE"
            )
            .otherwise("GOOD")
        )
    )

    # --------------------------------------------------
    # 9. Select Silver columns
    # --------------------------------------------------

    silver_columns = [
        "hvfhs_license_num",
        "dispatching_base_num",
        "originating_base_num",
        "request_datetime",
        "on_scene_datetime",
        "pickup_datetime",
        "dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_miles",
        "trip_time",
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
        "calculated_trip_time_seconds",
        "trip_time_consistent",

        "timestamp_quality_flag",
        "financial_quality_flag",
        "overall_quality_status",

        "_source_month",
        "_source_file",
        "_ingested_at"
    ]

    df_silver = df_final.select(silver_columns)

    # --------------------------------------------------
    # 10. Write to Silver
    # --------------------------------------------------

    if spark.catalog.tableExists(silver_table):

        (
            df_silver
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(silver_table)
        )

    else:

        (
            df_silver
            .write
            .format("delta")
            .mode("overwrite")
            .partitionBy("_source_month")
            .saveAsTable(silver_table)
        )

    print(
        f"{source_month} successfully written to Silver."
    )

    return df_silver