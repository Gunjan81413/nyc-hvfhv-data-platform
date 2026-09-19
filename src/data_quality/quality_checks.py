from pyspark.sql import functions as F


def run_data_quality_checks(
    spark,
    source_month,
    silver_table,
    fact_table,
    daily_gold_table,
    location_gold_table,
    shared_gold_table
):
    """
    Run reusable data quality checks for one source month.
    """

    print("=" * 60)
    print(f"DATA QUALITY CHECKS: {source_month}")
    print("=" * 60)

    results = []

    # ---------------------------------------------------------
    # 1. SILVER ROW COUNT
    # ---------------------------------------------------------

    df_silver = (
        spark.table(silver_table)
        .filter(F.col("_source_month") == source_month)
    )

    silver_count = df_silver.count()

    print(f"\nSilver rows: {silver_count:,}")

    if silver_count > 0:
        results.append(("Silver row count", "PASS"))
    else:
        results.append(("Silver row count", "FAIL"))

    # ---------------------------------------------------------
    # 2. FACT ROW COUNT
    # ---------------------------------------------------------

    df_fact = (
        spark.table(fact_table)
        .filter(F.col("_source_month") == source_month)
    )

    fact_count = df_fact.count()

    print(f"Fact rows:   {fact_count:,}")

    if fact_count == silver_count:
        results.append(("Silver → Fact row reconciliation", "PASS"))
    else:
        results.append(("Silver → Fact row reconciliation", "FAIL"))

    # ---------------------------------------------------------
    # 3. DUPLICATE TRIP KEYS
    # ---------------------------------------------------------

    duplicate_trip_keys = (
        df_fact
        .groupBy("trip_key")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"Duplicate trip keys: {duplicate_trip_keys:,}")

    if duplicate_trip_keys == 0:
        results.append(("Duplicate trip key check", "PASS"))
    else:
        results.append(("Duplicate trip key check", "FAIL"))

    # ---------------------------------------------------------
    # 4. PROVIDER KEY COVERAGE
    # ---------------------------------------------------------

    missing_provider_keys = (
        df_fact
        .filter(F.col("provider_key").isNull())
        .count()
    )

    print(f"Missing provider keys: {missing_provider_keys:,}")

    if missing_provider_keys == 0:
        results.append(("Provider key coverage", "PASS"))
    else:
        results.append(("Provider key coverage", "FAIL"))

    # ---------------------------------------------------------
    # 5. LOCATION KEY COVERAGE
    # ---------------------------------------------------------

    missing_pickup_locations = (
        df_fact
        .filter(F.col("pickup_location_key").isNull())
        .count()
    )

    missing_dropoff_locations = (
        df_fact
        .filter(F.col("dropoff_location_key").isNull())
        .count()
    )

    print(
        f"Missing pickup location keys: "
        f"{missing_pickup_locations:,}"
    )

    print(
        f"Missing dropoff location keys: "
        f"{missing_dropoff_locations:,}"
    )

    if (
        missing_pickup_locations == 0
        and missing_dropoff_locations == 0
    ):
        results.append(("Location key coverage", "PASS"))
    else:
        results.append(("Location key coverage", "FAIL"))

    # ---------------------------------------------------------
    # 6. DISPATCHING KEY COVERAGE
    # ---------------------------------------------------------

    missing_dispatching_keys = (
        df_fact
        .filter(F.col("dispatching_base_key").isNull())
        .count()
    )

    print(
        f"Missing dispatching keys: "
        f"{missing_dispatching_keys:,}"
    )

    if missing_dispatching_keys == 0:
        results.append(("Dispatching key coverage", "PASS"))
    else:
        results.append(("Dispatching key coverage", "FAIL"))

    # ---------------------------------------------------------
    # 7. TIMESTAMP QUALITY
    # ---------------------------------------------------------

    timestamp_issues = (
        df_fact
        .filter(F.col("timestamp_quality_flag") == "ISSUE")
        .count()
    )

    print(f"Timestamp issue trips: {timestamp_issues:,}")

    # We retain these records rather than deleting them.
    # Therefore this is informational rather than a hard failure.
    if timestamp_issues == 0:
        results.append(("Timestamp quality check", "PASS"))
    else:
        results.append(
            (f"Timestamp anomalies detected: {timestamp_issues:,}", "WARNING")
        )

    # ---------------------------------------------------------
    # 8. FINANCIAL QUALITY
    # ---------------------------------------------------------

    financial_issues = (
        df_fact
        .filter(F.col("financial_quality_flag") == "ISSUE")
        .count()
    )

    print(f"Financial issue trips: {financial_issues:,}")

    # Same principle: records are retained and flagged.
    if financial_issues == 0:
        results.append(("Financial quality check", "PASS"))
    else:
        results.append(
            (f"Financial anomalies detected: {financial_issues:,}", "WARNING")
        )

    # ---------------------------------------------------------
    # 9. DAILY PROVIDER GOLD RECONCILIATION
    # ---------------------------------------------------------

    daily_gold = (
        spark.table(daily_gold_table)
        .filter(
            F.date_format("date", "yyyy-MM") == source_month
        )
    )

    daily_gold_trips = (
        daily_gold
        .agg(F.sum("total_trips"))
        .first()[0]
    )

    print(f"Daily Provider Gold trips: {daily_gold_trips:,}")

    if daily_gold_trips == fact_count:
        results.append(("Daily Provider Gold reconciliation", "PASS"))
    else:
        results.append(("Daily Provider Gold reconciliation", "FAIL"))

    # ---------------------------------------------------------
    # 10. LOCATION GOLD RECONCILIATION
    # ---------------------------------------------------------

    location_gold = spark.table(location_gold_table)

    location_gold_trips = (
        location_gold
        .agg(F.sum("total_trips"))
        .first()[0]
    )

    print(f"Location Gold trips: {location_gold_trips:,}")

    # Location Gold is rebuilt from the complete Fact table.
    total_fact_trips = spark.table(fact_table).count()

    if location_gold_trips == total_fact_trips:
        results.append(("Location Gold reconciliation", "PASS"))
    else:
        results.append(("Location Gold reconciliation", "FAIL"))

    # ---------------------------------------------------------
    # 11. SHARED RIDE GOLD RECONCILIATION
    # ---------------------------------------------------------

    shared_gold = spark.table(shared_gold_table)

    shared_gold_trips = (
        shared_gold
        .agg(F.sum("total_trips"))
        .first()[0]
    )

    print(f"Shared Ride Gold trips: {shared_gold_trips:,}")

    if shared_gold_trips == total_fact_trips:
        results.append(("Shared Ride Gold reconciliation", "PASS"))
    else:
        results.append(("Shared Ride Gold reconciliation", "FAIL"))

    # ---------------------------------------------------------
    # FINAL REPORT
    # ---------------------------------------------------------

    print("\n" + "=" * 60)
    print("DATA QUALITY SUMMARY")
    print("=" * 60)

    for check_name, status in results:
        print(f"{status:5} | {check_name}")

    failed_checks = [
        check_name
        for check_name, status in results
        if status == "FAIL"
    ]

    print("\n" + "=" * 60)

    if failed_checks:
        print("DATA QUALITY RESULT: FAILED")

        print("Failed checks:")
        for check in failed_checks:
            print(f" - {check}")

    else:
        print("DATA QUALITY RESULT: PASSED")

        warnings = [
            check_name
            for check_name, status in results
            if status == "WARNING"
        ]

        if warnings:
            print(f"Warnings detected: {len(warnings)}")

    print("=" * 60)

    return results