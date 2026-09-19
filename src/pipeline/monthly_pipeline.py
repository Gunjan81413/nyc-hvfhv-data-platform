# src/pipeline/monthly_pipeline.py

from ingestion.bronze import ingest_month_to_bronze
from transformation.silver import transform_month_to_silver
from transformation.dispatch import update_dispatch_dimension
from transformation.fact import build_month_to_fact

from transformation.gold import (
    build_daily_provider_gold,
    build_location_gold,
    build_shared_ride_gold
)

def run_month(spark, source_month):
    """
    Run the complete monthly NYC HVFHV pipeline.

    Flow:
    Bronze → Silver → Dimension Update → Fact → Gold
    """

    print("=" * 60)
    print(f"STARTING MONTHLY PIPELINE: {source_month}")
    print("=" * 60)

    # ---------------------------------------------------------
    # TABLES / PATHS
    # ---------------------------------------------------------

    RAW_BASE_PATH = "/Volumes/workspace/default/nyc_hvfhv_data"

    BRONZE_TABLE = "workspace.default.bronze_hvfhv_trips"
    SILVER_TABLE = "workspace.default.silver_hvfhv_trips"

    DISPATCH_TABLE = "workspace.default.dim_dispatch"
    PROVIDER_TABLE = "workspace.default.dim_provider"
    LOCATION_TABLE = "workspace.default.dim_location"
    DATE_TABLE = "workspace.default.dim_date"

    FACT_TABLE = "workspace.default.fact_trip"

    DAILY_PROVIDER_GOLD = (
        "workspace.default.gold_daily_provider_metrics"
    )

    LOCATION_GOLD = (
        "workspace.default.gold_location_metrics"
    )

    SHARED_RIDE_GOLD = (
        "workspace.default.gold_shared_ride_metrics"
    )

    # ---------------------------------------------------------
    # 1. BRONZE
    # ---------------------------------------------------------

    print("\n[1/6] BRONZE INGESTION")

    ingest_month_to_bronze(
        spark,
        RAW_BASE_PATH,
        source_month,
        BRONZE_TABLE
    )

    # ---------------------------------------------------------
    # 2. SILVER
    # ---------------------------------------------------------

    print("\n[2/6] SILVER TRANSFORMATION")

    transform_month_to_silver(
        spark,
        source_month,
        BRONZE_TABLE,
        SILVER_TABLE
    )

    # ---------------------------------------------------------
    # 3. UPDATE DISPATCH DIMENSION
    # ---------------------------------------------------------

    print("\n[3/6] DISPATCH DIMENSION")

    update_dispatch_dimension(
        spark,
        source_month,
        SILVER_TABLE,
        DISPATCH_TABLE
    )

    # ---------------------------------------------------------
    # 4. FACT
    # ---------------------------------------------------------

    print("\n[4/6] FACT TABLE")

    build_month_to_fact(
        spark,
        source_month,
        SILVER_TABLE,
        DATE_TABLE,
        PROVIDER_TABLE,
        LOCATION_TABLE,
        DISPATCH_TABLE,
        FACT_TABLE
    )

    # ---------------------------------------------------------
    # 5. GOLD — DAILY PROVIDER
    # ---------------------------------------------------------

    print("\n[5/6] DAILY PROVIDER GOLD")

    build_daily_provider_gold(
        spark,
        source_month,
        FACT_TABLE,
        PROVIDER_TABLE,
        DATE_TABLE,
        DAILY_PROVIDER_GOLD
    )

    # ---------------------------------------------------------
    # 6. GOLD — LOCATION + SHARED RIDE
    # ---------------------------------------------------------

    print("\n[6/6] LOCATION + SHARED RIDE GOLD")

    build_location_gold(
        spark,
        FACT_TABLE,
        LOCATION_TABLE,
        LOCATION_GOLD
    )

    build_shared_ride_gold(
        spark,
        FACT_TABLE,
        PROVIDER_TABLE,
        SHARED_RIDE_GOLD
    )

    print("\n" + "=" * 60)
    print(f"PIPELINE COMPLETED: {source_month}")
    print("=" * 60)